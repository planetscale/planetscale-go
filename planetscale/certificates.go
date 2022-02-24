package planetscale

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type DatabaseBranchCertificate struct {
	PublicID    string         `json:"id"`
	Name        string         `json:"display_name"`
	Role        string         `json:"role"`
	Branch      DatabaseBranch `json:"database_branch"`
	Certificate string         `json:"certificate"`
	CreatedAt   time.Time      `json:"created_at"`
	DeletedAt   time.Time      `json:"deleted_at"`
}

func (c *DatabaseBranchCertificate) X509KeyPair(r *DatabaseBranchCertificateRequest) (tls.Certificate, error) {
	if r.PrivateKey == nil {
		return tls.Certificate{}, errors.New("certificate request does not contain a private key")
	}
	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(r.PrivateKey)
	if err != nil {
		return tls.Certificate{}, errors.Wrap(err, "failed to marshal private key")
	}

	privateKey := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privateKeyBytes,
		},
	)

	cert, err := tls.X509KeyPair([]byte(c.Certificate), privateKey)
	if err != nil {
		return cert, errors.Wrap(err, "parsing client certificate failed")
	}
	return cert, nil
}

// DatabaseBranchCertificateRequest encapsulates the request for creating/getting/deleting a
// database branch certificate.
type DatabaseBranchCertificateRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	DisplayName  string `json:"display_name"`
	Role         string `json:"role"`
	PrivateKey   crypto.PrivateKey
}

// ListDatabaseBranchCertificateRequest encapsulates the request for listing all certificates
// for a given database branch.
type ListDatabaseBranchCertificateRequest struct {
	Organization string
	Database     string
	Branch       string
}

type certificatesResponse struct {
	Certificates []*DatabaseBranchCertificate `json:"data"`
}

// GetDatabaseBranchCertificateRequest encapsulates the request for listing all certificates
// for a given database branch.
type GetDatabaseBranchCertificateRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	DisplayName  string `json:"display_name"`
	PasswordId   string
}

// CertificatesService is an interface for communicating with the PlanetScale
// Database Branch Passwords API endpoint.
type CertificatesService interface {
	Create(context.Context, *DatabaseBranchCertificateRequest) (*DatabaseBranchCertificate, error)
	List(context.Context, *ListDatabaseBranchCertificateRequest) ([]*DatabaseBranchCertificate, error)
	Get(context.Context, *GetDatabaseBranchCertificateRequest) (*DatabaseBranchCertificate, error)
}

type certificatesService struct {
	client *Client
}

var _ CertificatesService = &certificatesService{}

func NewCertificatesService(client *Client) *certificatesService {
	return &certificatesService{
		client: client,
	}
}

func (c *certificatesService) Create(ctx context.Context, r *DatabaseBranchCertificateRequest) (*DatabaseBranchCertificate, error) {
	switch priv := r.PrivateKey.(type) {
	case *rsa.PrivateKey:
	case *ecdsa.PrivateKey:
	case nil:
		return nil, errors.New("no private key given")
	default:
		return nil, errors.Errorf("unsupported key type: %T, only supports ECDSA and RSA private keys", priv)
	}

	template := x509.CertificateRequest{
		Version: 1,
	}

	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, &template, r.PrivateKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create csr")
	}

	var buf bytes.Buffer
	err = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrBytes})
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode the CSR to PEM")
	}

	var certReq = struct {
		CSR         string `json:"csr"`
		Role        string `json:"role"`
		DisplayName string `json:"display_name"`
	}{
		CSR:         buf.String(),
		Role:        r.Role,
		DisplayName: r.DisplayName,
	}

	req, err := c.client.newRequest(
		http.MethodPost,
		certificatesBranchAPIPath(r.Organization, r.Database, r.Branch),
		certReq,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for create certificates")
	}

	certificate := &DatabaseBranchCertificate{}

	err = c.client.do(ctx, req, &certificate)
	if err != nil {
		return nil, err
	}

	return certificate, nil
}

// Get an existing password for a branch.
func (d *certificatesService) Get(ctx context.Context, getReq *GetDatabaseBranchCertificateRequest) (*DatabaseBranchCertificate, error) {
	path := certificateBranchAPIPath(getReq.Organization, getReq.Database, getReq.Branch, getReq.DisplayName)
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	certificate := &DatabaseBranchCertificate{}
	if err := d.client.do(ctx, req, &certificate); err != nil {
		return nil, err
	}

	return certificate, nil
}

// List all existing passwords for a specific branch.
func (d *certificatesService) List(ctx context.Context, listReq *ListDatabaseBranchCertificateRequest) ([]*DatabaseBranchCertificate, error) {
	path := certificatesBranchAPIPath(listReq.Organization, listReq.Database, listReq.Branch)

	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request to list certificates")
	}

	certificatesResp := &certificatesResponse{}
	if err := d.client.do(ctx, req, &certificatesResp); err != nil {
		return nil, err
	}

	return certificatesResp.Certificates, nil
}

func certificatesBranchAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/certificates", databaseBranchAPIPath(org, db, branch))
}

func certificateBranchAPIPath(org, db, branch, password string) string {
	return fmt.Sprintf("%s/%s", certificatesBranchAPIPath(org, db, branch), password)
}
