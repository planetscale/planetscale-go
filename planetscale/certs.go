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
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"net/http"
)

type CreateCertificateRequest struct {
	Organization string
	DatabaseName string
	Branch       string

	// PrivateKey is used to generate the Certificate Sign Request (CSR).
	PrivateKey crypto.PrivateKey
}

type CertificatesService interface {
	Create(context.Context, *CreateCertificateRequest) (*Cert, error)
}

type Cert struct {
	ClientCert tls.Certificate
	AccessHost string
	Ports      RemotePorts
}

type RemotePorts struct {
	Proxy int
	MySQL int
}

type certificatesService struct {
	client *Client
}

var _ CertificatesService = &certificatesService{}

func NewCertsService(client *Client) *certificatesService {
	return &certificatesService{
		client: client,
	}
}

func (c *certificatesService) Create(ctx context.Context, r *CreateCertificateRequest) (*Cert, error) {
	cn := fmt.Sprintf("%s/%s/%s", r.Organization, r.DatabaseName, r.Branch)
	subj := pkix.Name{
		CommonName: cn,
	}

	switch priv := r.PrivateKey.(type) {
	case *rsa.PrivateKey:
	case *ecdsa.PrivateKey:
	default:
		return nil, fmt.Errorf("unsupported key type: %T, only supports ECDSA and RSA private keys", priv)
	}

	template := x509.CertificateRequest{
		Version: 1,
		Subject: subj,
	}

	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, &template, r.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("unable to create csr: %s", err)
	}

	var buf bytes.Buffer
	err = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrBytes})
	if err != nil {
		return nil, fmt.Errorf("unable to encode the CSR to PEM: %s", err)
	}

	var certReq = struct {
		CSR string `json:"csr"`
	}{
		CSR: buf.String(),
	}

	req, err := c.client.newRequest(
		http.MethodPost,
		fmt.Sprintf("%s/%s/branches/%s/create-certificate",
			databasesAPIPath(r.Organization),
			r.DatabaseName,
			r.Branch,
		),
		certReq,
	)
	if err != nil {
		return nil, fmt.Errorf("error creating request for create certificates: %s", err)
	}

	var cr struct {
		Certificate string         `json:"certificate"`
		AccessHost  string         `json:"access_host"`
		Ports       map[string]int `json:"ports"`
	}

	err = c.client.do(ctx, req, &cr)
	if err != nil {
		return nil, err
	}

	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(r.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %s", err)
	}

	privateKey := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privateKeyBytes,
		},
	)

	clientCert, err := tls.X509KeyPair([]byte(cr.Certificate), privateKey)
	if err != nil {
		return nil, fmt.Errorf("parsing client certificate failed: %s", err)
	}

	return &Cert{
		ClientCert: clientCert,
		AccessHost: cr.AccessHost,
		Ports: RemotePorts{
			Proxy: cr.Ports["proxy"],
			MySQL: cr.Ports["mysql-tls"],
		},
	}, nil
}
