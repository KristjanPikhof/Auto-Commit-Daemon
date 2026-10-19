//go:build integration

package integration_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// headSubject returns the HEAD commit subject (`%s` in git log) for repo.
func headSubject(t *testing.T, repo string) string {
	t.Helper()
	out := runGitOK(t, repo, "log", "-1", "--pretty=%s", "HEAD")
	return strings.TrimSpace(out)
}

// commitCount returns the integer count of commits reachable from HEAD.
func commitCount(t *testing.T, repo string) int {
	t.Helper()
	out := runGitOK(t, repo, "rev-list", "--count", "HEAD")
	n := 0
	fmt.Sscanf(strings.TrimSpace(out), "%d", &n)
	return n
}

// newOpenAITestServer returns an HTTPS httptest server plus the ACD_AI_CA_FILE
// override that lets the integration binary trust its private test CA.
func newOpenAITestServer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	now := time.Now()
	caTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ACD integration test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test CA cert: %v", err)
	}
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test server key: %v", err)
	}
	leafTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, &leafTemplate, &caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test server cert: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{leafDER, caDER},
			PrivateKey:  leafKey,
		}},
		MinVersion: tls.VersionTLS12,
	}
	server.StartTLS()
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caDER,
	})
	certPath := filepath.Join(t.TempDir(), "openai-test-ca.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		server.Close()
		t.Fatalf("write OpenAI test CA: %v", err)
	}
	return server, "ACD_AI_CA_FILE=" + certPath
}
