package planetscale

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

const testCertificateID = "9nWlLMXw7twk"
const testCert = `-----BEGIN CERTIFICATE-----
MIIB+jCCAaCgAwIBAgIJAM1hLEO/Mf1sMAoGCCqGSM49BAMCMDIxMDAuBgNVBAMM
J1BsYW5ldFNjYWxlIEF1dGhlbnRpY2F0aW9uIEludGVybWVkaWF0ZTAeFw0yMTA3
MDcxMjMxNTdaFw0yMTA4MDYxMjMxNTdaMDExLzAtBgNVBAMMJlBsYW5ldFNjYWxl
IEF1dGhlbnRpY2F0aW9uIENlcnRpZmljYXRlMFkwEwYHKoZIzj0CAQYIKoZIzj0D
AQcDQgAEJU6xHqUmOtoSatja0CZ3Y53EzzGNE/M32jAY+vw1LS+a0rXclAr7BruA
jhxQ4Kijpy+IkJfgF3zYWZnhzZ00sKOBnzCBnDAOBgNVHQ8BAf8EBAMCB4AwDAYD
VR0TAQH/BAIwADAgBgNVHSUBAf8EFjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwHQYD
VR0OBBYEFM/sE86SfWrr0ZdFcqQi8bny17ZeMDsGA1UdEQQ0MDKGMHNwaWZmZTov
L3BsYW5ldHNjYWxlLmNvbS9zb21lLXVzZXIvc29tZS1kYXRhYmFzZTAKBggqhkjO
PQQDAgNIADBFAiB9x86B2A6n8w+FDxeHke32+3nDyzhQWAbcmyO6u0orOgIhAPOO
7ne03mib2Y1NaKNIFlaMRYYHA7NAFmUKyrB/l+jL
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIB/jCCAYOgAwIBAgIJAJM5ZRpB0eppMAoGCCqGSM49BAMCMCoxKDAmBgNVBAMM
H1BsYW5ldFNjYWxlIEF1dGhlbnRpY2F0aW9uIFJvb3QwHhcNMjEwNzA4MTMxMDAz
WhcNMjYwNzA3MTMxMDAzWjAyMTAwLgYDVQQDDCdQbGFuZXRTY2FsZSBBdXRoZW50
aWNhdGlvbiBJbnRlcm1lZGlhdGUwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARD
aMH0uN0j0RMMutakbvrbcAnrctjZqsXxfyyQb4sWDc5d3jkQa9QwbBygEhTZwMCb
9OJzks2XBar9Re4qY6m4o4GJMIGGMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8E
BTADAQH/MB0GA1UdDgQWBBTFCW8sT0y8xwFSsql+G83KgPCR5DAfBgNVHSMEGDAW
gBQDYaBNEwr31OAXyYfb1LkoysyDQTAjBgNVHREEHDAahhhzcGlmZmU6Ly9wbGFu
ZXRzY2FsZS5jb20wCgYIKoZIzj0EAwIDaQAwZgIxAPe1GVe9anAhR02+qRf6u2+Z
cjaVH/gRhl6LweVTb2YUPxrZ+gtwhx0oAx9p2sQa2QIxAJPUIXyCaLa1e5qIpv16
3VGE4ZIOlqPmLokx0TfZJP/3UbA8IwSEdGQUVEtn2AUKxQ==
-----END CERTIFICATE-----`

const testKey = `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg2Xhd7IaDvRBJKaYv
Lj0WsQVMtFDmG24kHL5wsij8Hz6hRANCAAQlTrEepSY62hJq2NrQJndjncTPMY0T
8zfaMBj6/DUtL5rStdyUCvsGu4COHFDgqKOnL4iQl+AXfNhZmeHNnTSw
-----END PRIVATE KEY-----`

func TestCertificates_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)

		var out = struct {
			ID          string `json:"id"`
			Certificate string `json:"certificate"`
			DisplayName string `json:"display_name"`
			CreatedAt   string `json:"created_at"`
			Role        string `json:"role"`
		}{
			ID:          testCertificateID,
			Certificate: testCert,
			DisplayName: "planetscale-go-test-certificate",
			CreatedAt:   "2021-01-14T10:19:23.000Z",
			Role:        "writer",
		}

		err := json.NewEncoder(w).Encode(out)
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	privateDer, _ := pem.Decode([]byte(testKey))

	privateKey, err := x509.ParsePKCS8PrivateKey(privateDer.Bytes)
	c.Assert(err, qt.IsNil)

	req := &DatabaseBranchCertificateRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		Role:         "writer",
		PrivateKey:   privateKey,
	}
	certificate, err := client.Certificates.Create(ctx, req)

	want := &DatabaseBranchCertificate{
		Name:        "planetscale-go-test-certificate",
		PublicID:    testCertificateID,
		CreatedAt:   time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		Role:        "writer",
		Certificate: testCert,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(certificate, qt.DeepEquals, want)

	tlsCert, err := certificate.X509KeyPair(req)
	c.Assert(err, qt.IsNil)
	c.Assert(tlsCert.PrivateKey, qt.DeepEquals, req.PrivateKey)
	c.Assert(tlsCert.Certificate, qt.HasLen, 2)
}

func TestCertificates_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)

		type entry struct {
			ID          string `json:"id"`
			Certificate string `json:"certificate"`
			DisplayName string `json:"display_name"`
			CreatedAt   string `json:"created_at"`
			Role        string `json:"role"`
		}
		var out = struct {
			Data []entry `json:"data"`
		}{[]entry{{
			ID:          testCertificateID,
			Certificate: testCert,
			DisplayName: "planetscale-go-test-certificate",
			CreatedAt:   "2021-01-14T10:19:23.000Z",
			Role:        "writer",
		}}}

		err := json.NewEncoder(w).Encode(out)
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	certificates, err := client.Certificates.List(ctx, &ListDatabaseBranchCertificateRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	})

	want := []*DatabaseBranchCertificate{
		{
			Name:        "planetscale-go-test-certificate",
			PublicID:    testCertificateID,
			CreatedAt:   time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
			Role:        "writer",
			Certificate: testCert,
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(certificates, qt.DeepEquals, want)
}

func TestCertificates_ListEmpty(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	certificates, err := client.Certificates.List(ctx, &ListDatabaseBranchCertificateRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(certificates, qt.HasLen, 0)
}

func TestCertificates_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)

		var out = struct {
			ID          string `json:"id"`
			Certificate string `json:"certificate"`
			DisplayName string `json:"display_name"`
			CreatedAt   string `json:"created_at"`
			Role        string `json:"role"`
		}{
			ID:          testCertificateID,
			Certificate: testCert,
			DisplayName: "planetscale-go-test-certificate",
			CreatedAt:   "2021-01-14T10:19:23.000Z",
			Role:        "writer",
		}
		err := json.NewEncoder(w).Encode(out)
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "planetscale-go-test-db"
	branch := "my-branch"

	certificate, err := client.Certificates.Get(ctx, &GetDatabaseBranchCertificateRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		PasswordId:   testPasswordID,
	})

	want := &DatabaseBranchCertificate{
		Name:        "planetscale-go-test-certificate",
		PublicID:    testCertificateID,
		CreatedAt:   time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		Role:        "writer",
		Certificate: testCert,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(certificate, qt.DeepEquals, want)
}
