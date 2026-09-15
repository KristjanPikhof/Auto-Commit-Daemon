package cli

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)





































ty////fufufufufutyfufufufufufufufu//fufu//fufufufufufufufunc shortenSHA(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "(none)"
	}
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
