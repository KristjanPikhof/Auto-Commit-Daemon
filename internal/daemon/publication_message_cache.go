package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const publicationMessageCacheMeta = "intent.v2.message_cache"

type cachedPublicationMessage struct {
	Result    ai.Result `json:"result"`
	CreatedTS float64   `json:"created_ts"`
}

// The worker loads and persists this snapshot. Only the isolated evaluation
// accesses entries between those boundaries, including partially successful
// rewrites when a later group fails.
type publicationMessageCache struct {
	provider interface{ Name() string }
	identity string
	entries  map[string]cachedPublicationMessage
	dirty    bool
}

func (c *publicationMessageCache) Name() string { return c.provider.Name() }
func (c *publicationMessageCache) RewriteIntentMessage(ctx context.Context, req ai.IntentMessageRewriteRequest) (ai.Result, error) {
	data, err := json.Marshal([]any{c.identity, req})
	if err != nil {
		return ai.Result{}, err
	}
	key := fmt.Sprintf("%x", sha256.Sum256(data))
	if cached, ok := c.entries[key]; ok {
		return cached.Result, nil
	}
	result, err := c.provider.(ai.IntentMessageRewriter).RewriteIntentMessage(ctx, req)
	if err != nil {
		return result, err
	}
	checked := req.LockedPlan
	checked.Subject, checked.Body = result.Subject, result.Body
	quality := ai.EvaluateIntentPlanMessageQuality(req.PlannerRequest, checked)
	if quality.Action == ai.MessageQualityClean || quality.Action == ai.MessageQualitySanitizeAccept {
		result.Subject, result.Body = quality.SanitizedSubject, quality.SanitizedBody
		if len(result.Subject)+len(result.Body) <= 16384 {
			c.entries[key] = cachedPublicationMessage{Result: result, CreatedTS: float64(time.Now().UnixNano()) / 1e9}
			c.dirty = true
		}
	}
	return result, nil
}
func loadPublicationMessageCache(ctx context.Context, provider interface{ Name() string }, health *IntentPlannerHealth) (*publicationMessageCache, error) {
	cache := &publicationMessageCache{provider: provider, identity: provider.Name(), entries: map[string]cachedPublicationMessage{}}
	if health != nil {
		cache.identity = health.Snapshot().ProviderFingerprint
	}
	evaluation, _ := ctx.Value(publicationEvaluationKey{}).(*publicationEvaluation)
	if evaluation == nil || evaluation.db == nil {
		return cache, nil
	}
	runtime, err := state.RuntimeConfigActivationState(ctx, evaluation.db)
	if err != nil {
		return nil, err
	}
	cache.identity = fmt.Sprintf("%s/%v/%v", cache.identity, runtime.DesiredRevisionID, runtime.AppliedRevisionID)
	raw, found, err := state.MetaGet(ctx, evaluation.db, publicationMessageCacheMeta)
	if err != nil {
		return nil, err
	}
	if found && json.Unmarshal([]byte(raw), &cache.entries) != nil {
		cache.entries = nil
	}
	if cache.entries == nil {
		cache.entries = map[string]cachedPublicationMessage{}
	}
	return cache, nil
}
func (c *publicationMessageCache) persist(ctx context.Context) error {
	evaluation, _ := ctx.Value(publicationEvaluationKey{}).(*publicationEvaluation)
	if !c.dirty || evaluation == nil || evaluation.db == nil {
		return nil
	}
	for len(c.entries) > 32 {
		oldest := ""
		for key, record := range c.entries {
			if oldest == "" || record.CreatedTS < c.entries[oldest].CreatedTS || record.CreatedTS == c.entries[oldest].CreatedTS && key < oldest {
				oldest = key
			}
		}
		delete(c.entries, oldest)
	}
	return state.MetaSetJSON(ctx, evaluation.db, publicationMessageCacheMeta, c.entries)
}

// Old runtime revisions may still use v1 grouping. Their local safety fallback
// follows the same selected-provider message contract as current Intent.
func rewriteLegacyFallbackMessages(ctx context.Context, planner ai.IntentPlanner, health *IntentPlannerHealth, req ai.IntentPlanRequest, plan ai.IntentPlan) (ai.IntentPlan, error) {
	if _, ok := planner.(ai.IntentMessageRewriter); !ok {
		return ai.IntentPlan{}, &IntentPlannerCircuitOpenError{RetryAt: time.Now().Add(30 * time.Second)}
	}
	cache, err := loadPublicationMessageCache(ctx, planner, health)
	if err != nil {
		return ai.IntentPlan{}, err
	}
	groups, err := ai.IntentPlanCommitGroups(plan)
	if err != nil {
		return ai.IntentPlan{}, err
	}
	for i, group := range groups {
		locked := ai.IntentPlanForCommitGroup(plan, group)
		request := ai.NewIntentMessageRewriteRequest(req, locked, ai.EvaluateIntentPlanMessageQuality(req, locked))
		var result ai.Result
		_, err := generatePublicationMessage(ctx, func(jobCtx context.Context, _ EventContext) (string, error) {
			var err error
			result, err = cache.RewriteIntentMessage(jobCtx, request)
			if err != nil {
				return "", err
			}
			locked.Subject, locked.Body = result.Subject, result.Body
			quality := ai.EvaluateIntentPlanMessageQuality(req, locked)
			if quality.Action != ai.MessageQualityClean && quality.Action != ai.MessageQualitySanitizeAccept {
				return "", &ai.IntentMessageRewriteValidationError{Err: fmt.Errorf("legacy fallback message needs correction")}
			}
			return result.Subject, nil
		}, EventContext{}, health)
		if cacheErr := cache.persist(ctx); cacheErr != nil {
			return ai.IntentPlan{}, cacheErr
		}
		if err != nil {
			return ai.IntentPlan{}, err
		}
		groups[i].Subject, groups[i].Body = result.Subject, result.Body
	}
	if len(plan.CommitGroups) > 0 {
		plan.CommitGroups = groups
	} else if len(groups) > 0 {
		plan.Subject, plan.Body = groups[0].Subject, groups[0].Body
	}
	plan.Source = planner.Name()
	return plan, nil
}
