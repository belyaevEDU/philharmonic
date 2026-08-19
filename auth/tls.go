package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

const minTLSVersion = tls.VersionTLS12

// certFile/keyFile are the server's own certificate pair (both or neither,
// an empty pair yields a nil config, resulting in plain HTTP).
// When clientCAFile is set, clients must present a certificate signed by that CA (mTLS).
// without it the server accepts any client (TLS w/o client auth)
func ServerTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("cert_file and key_file must be set together")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server certificate %s/%s: %w", certFile, keyFile, err)
	}

	cfg := &tls.Config{
		MinVersion:   minTLSVersion,
		Certificates: []tls.Certificate{cert},
	}

	if clientCAFile != "" {
		pool, err := LoadCAPool(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("loading client CA %s: %w", clientCAFile, err)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// caFile is the CA bundle used to verify server certificates. empty means
// the system roots. certFile/keyFile, when set, are presented to servers
// that require client certificates (mTLS). All-empty input yields a nil
// config, resulting in plain HTTP
func ClientTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("cert_file and key_file must be set together")
	}

	cfg := &tls.Config{MinVersion: minTLSVersion}

	if caFile != "" {
		pool, err := LoadCAPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("loading CA bundle %s: %w", caFile, err)
		}
		cfg.RootCAs = pool
	}

	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate %s/%s: %w", certFile, keyFile, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// reads PEM-encoded certificates into a verification pool
func LoadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) // #nosec G304, intended behaviour
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s contains no valid PEM certificates", path)
	}
	return pool, nil
}
