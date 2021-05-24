package dbutil

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"
)

// DialConfig defines the configuration to use to dial to a PlanetScale Database
type DialConfig struct {
	// Organization represents a PlanetScale organization.
	Organization string

	// Database defines the PlanetScale database to connect.
	Database string

	// Branch defines the PlanetScale branch to connect.
	Branch string

	// Client defines a PlanetScale client. Use planetscale.NewClient() to
	// create a new instance.
	Client *ps.Client
}

// Dial creates a secure connection to a PlanetScale database with the given
// configuration.
func Dial(ctx context.Context, cfg *DialConfig) (*sql.DB, error) {
	if cfg.Client == nil {
		return nil, errors.New("planetscale Client is not set")
	}

	pkey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("couldn't generate private key: %s", err)
	}

	remoteAddr, tlsConfig, err := createTLSConfig(ctx, cfg, pkey, cfg.Client.Certificates)
	if err != nil {
		return nil, err
	}

	key := "planetscale"
	err = mysql.RegisterTLSConfig(key, tlsConfig)
	if err != nil {
		return nil, err
	}
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.Addr = remoteAddr
	mysqlCfg.Net = "tcp"
	mysqlCfg.TLSConfig = key

	db, err := sql.Open("mysql", mysqlCfg.FormatDSN())
	if err == nil {
		err = db.PingContext(ctx)
	}

	return db, err
}

// createTLSConfig is an internal function that returns the remote address and
// tls.Config to be used to connect to a PlanetScale database.
func createTLSConfig(
	ctx context.Context,
	cfg *DialConfig,
	pkey *rsa.PrivateKey,
	certService ps.CertificatesService,
) (string, *tls.Config, error) {
	if cfg.Organization == "" {
		return "", nil, errors.New("organization is not set")
	}

	if cfg.Database == "" {
		return "", nil, errors.New("database is not set")
	}

	if cfg.Branch == "" {
		return "", nil, errors.New("branch is not set")
	}

	cert, err := certService.Create(ctx, &ps.CreateCertificateRequest{
		Organization: cfg.Organization,
		DatabaseName: cfg.Database,
		Branch:       cfg.Branch,
		PrivateKey:   pkey,
	})
	if err != nil {
		return "", nil, err
	}

	rootCertPool := x509.NewCertPool()
	rootCertPool.AddCert(cert.CACert)

	serverName := fmt.Sprintf("%s.%s.%s.%s", cfg.Branch, cfg.Database, cfg.Organization, cert.RemoteAddr)
	remoteAddr := net.JoinHostPort(serverName, strconv.Itoa(cert.Ports.MySQL))

	return remoteAddr, &tls.Config{
		RootCAs:      rootCertPool,
		Certificates: []tls.Certificate{cert.ClientCert},
		ServerName:   serverName,
		// We need to set InsecureSkipVerify to true due to
		// https://github.com/GoogleCloudPlatform/cloudsql-proxy/issues/194
		// https://tip.golang.org/doc/go1.11#crypto/x509
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			opts := x509.VerifyOptions{
				Roots:         rootCertPool,
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		},
	}, nil
}
