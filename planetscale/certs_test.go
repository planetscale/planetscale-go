package planetscale

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

var (
	testPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAoTevM6Gx0+TD5/co/ddfIsBTKiCmjE1CKARqb8QNGEGYu61V
k74HnZ6+i9FKVFKyYBsvETJM2XugGkHmAY85r2R5HWou9zcmLkM6X71As70ktQOH
NUXqRmbRSlsKbFnWw5MF7I5pSyh2WGGaLeheJDX9qTv092G+NCgupHKHyI4HK0wG
RtpLJtYHjdpoEM53Mh3d5JDA13BUJaWCc4DPR5wOdLahthTegsNWQql9csToy7nJ
5JhGY3NAHY3bKcRKkIg0gueYIw/AovTTJV+Zf+CIUoMUt5VvvnlRsk24gUWDuVQ1
qUKdEsyac4juKfatd9HihjXOzg2tJ/2n9/qTuQIDAQABAoIBAGd6R2E7itl3v2rX
UJ9FqtGyYm7q0BvDxw/KbcrZKpKEIBVuVzxiP58i8ijqJ+xhvA5FxHskLwF1ATl5
TLl5hcwXEEoaCpUw97e//OrQnYQAhlwNLK679nhDrFgugU00iM21Q5sneVv9V6C4
3O5UdICHiw4h5sUWHrB5jh6NSKwnwEjS4qswwIZhKdKNjpuLhP4lQ4E4WXiBs7eg
OEeuoVA6L6uDwmoJ9P1bLn3F5f9UAAQ4s91aCXmnUJeCxec/zJuGK0CRKznLJ3G4
LmuNS3NYdmW/PN4wX0J0p4cr1LxD2WQfFuOIII13t7Y/BEMxtxDYpXqAlWEczKjd
ckQw4iUCgYEA1Rk3+3IUdOUVASorgtU2+PODBI5KEybTMFN3tlfCYZC/fwZxK0Hx
rGSUduXtEeaR21DDSZ3ndG1cCHATsQVpWeuzfuC14j6/rhflru9ikqCMSfp5er6l
QGXqvLLUBvK7WsPqf1Tm/9tox7Ctei+5rs3nwtWAzqALBNQH+aafY9sCgYEAwayY
oY1Vu1amBVzUB7bjw3E+UPNrPnRECstL1NYGd2jt+oAZpS6LO5E46LEspN5dNQAS
2rknRP9RbUbJ+LZ9X9w5su7d6odAkHc1oJgw3zaZqoGCLRH2Ls4qe4kzjIrRVGSw
cQnnYvpACZ50Gb3kM3qo78UFaELiNyVcJBfAxPsCgYByvQRmj9Mx6ZK4sNMCu/jA
bKUz08VQsIvvrlF7zZ7s13o0U+ylRPlyQCmsJzrRc5s/QioUPkA8cRGnvWjs3KQP
9ZgNDcMBEZY1j8psuZoSpv1Ca+nyzCnAFeAhQAxnvVRhl7FwY++I/cNaGegeLQpG
c7mBL2IOXx/vtpagtjWGFwKBgE53Vv9c+7cCzBCwI1dcybqNTuoNNQ4AnPCinP6G
F+iZIpGzBLDfwplHpP7hiWziinDGrtze1wIlTyAu5fVWOkV0PAw6qr4yPf5Jzfha
sLI+tNNX1R3dgRhFfwC9/ZybQWQnxzSFBrIbIYbEI9WqEaKpt3gtIpuzPWOKR2J4
HSmxAoGBAKPUEj4XcMz7aj+Vj0M0CjEIdlbi8vTRGDLRZIU+NMef318BDUxuyhPH
UDbbLFgm1/tckL9xKi4es6n17LH2LoXCi13/pvXscQHFZifCHH8ml7QO2X31R4UG
2guX2Rmbpb/mX9kyIeLGS94BfK0YwRYjKaNVWM3E1aOp19++oLu4
-----END RSA PRIVATE KEY-----`

	testSignedPublicKey = `-----BEGIN CERTIFICATE-----
MIICuzCCAaMCFHLnUdZL1i+gyDYACOCVlf/kuKJ7MA0GCSqGSIb3DQEBCwUAMBAx
DjAMBgNVBAMMBW15LWNhMB4XDTIxMDExNDE0MjU0MFoXDTIyMDExNDE0MjU0MFow
JDEiMCAGA1UEAwwZb3JnLWZvby9kYi1mb28vYnJhbmNoLWZvbzCCASIwDQYJKoZI
hvcNAQEBBQADggEPADCCAQoCggEBAKE3rzOhsdPkw+f3KP3XXyLAUyogpoxNQigE
am/EDRhBmLutVZO+B52evovRSlRSsmAbLxEyTNl7oBpB5gGPOa9keR1qLvc3Ji5D
Ol+9QLO9JLUDhzVF6kZm0UpbCmxZ1sOTBeyOaUsodlhhmi3oXiQ1/ak79PdhvjQo
LqRyh8iOBytMBkbaSybWB43aaBDOdzId3eSQwNdwVCWlgnOAz0ecDnS2obYU3oLD
VkKpfXLE6Mu5yeSYRmNzQB2N2ynESpCINILnmCMPwKL00yVfmX/giFKDFLeVb755
UbJNuIFFg7lUNalCnRLMmnOI7in2rXfR4oY1zs4NrSf9p/f6k7kCAwEAATANBgkq
hkiG9w0BAQsFAAOCAQEAzYwmGPb0qaQtK07I+4u1G0+DlwK+aWc/D3oLC/rF9XPq
mlh48nTacsJJF12VtlkQI+t6Mjw8F1CYQjeWlUMq5aItZKXGgiNRvT1LmqMNwSWA
J4hqgsGoBlP8WKRls6W7AmiK8cvd3sxAwFble4QGtmRb1QLTBoYdt5Fxd97+M57t
iveAOhvMQCp7sNbUQCYvugzFIc5ScGDQTho0sCXXPahhuhHjy3tG1JG1fwYRyV+H
MMSi245zV4dLChwoDpEkwODUHiR2TEv+q+T4fyP+zHISOKMdW15nIGjBuNByTzII
jncAhUUJgqpbMBTA3oHy1gZs/6wTCOWyz7E0LtlxCw==
-----END CERTIFICATE-----`

	testCACert = `-----BEGIN CERTIFICATE-----
MIIDATCCAemgAwIBAgIUGrBsGrF/ecgMfMzNB8/ARHPFkRswDQYJKoZIhvcNAQEL
BQAwEDEOMAwGA1UEAwwFbXktY2EwHhcNMjEwMTE0MTQyMDIwWhcNMjIwMTE0MTQy
MDIwWjAQMQ4wDAYDVQQDDAVteS1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBANPN0hhlHco+BEM0Yd3U3Pa3ZC+M0wMCA5HrKcTaxCw5xGs05W/+Ti0P
EPg6a5yymx41Z1KdPWsRqvjDZtZgdjwt0wSIFoLNvssrbXHRLxe5tv7PBLLMrHdN
bzKAibsganF4ZUC4PhjVmk9rE94NGvUQZRL5nK//fgktlaQHUWEhihWb/XAS62F6
/tiHyOLg/mUNAB6M64B5iSrHyYsqtH5oZXXSIeBUYtgtuSRF2uyhgLuoVwYA8NUC
fBn+sruZhD1sAFbTCuObU9zoA7UAQCdtmNpLQUZWxzR05qjVT9ydMIncG/RBOPaH
hT/1TJnrpEb7dhgaE8WHm9NOV/Gqp5cCAwEAAaNTMFEwHQYDVR0OBBYEFIsGnUu1
J/jeGbUK2rgbJZwjoygmMB8GA1UdIwQYMBaAFIsGnUu1J/jeGbUK2rgbJZwjoygm
MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAMy+H4kauCd/1n31
WnFS1k07ZupxUQrZJNYU83ofWQOff9ni2e6klzWjvm8v443iz20naACLNGK5oD8j
x3J+xdrRvEMgmChLVXDUh2e6HmCVhvytM3VXVqoXOzMXjv3UH6zzTO8DFLoF4D/f
Ym0qkgv2pOoyUe+ortHb+j2JMWma+GJgs3X7RpHduqqb8zxFIBfW3I/KbFfpOStC
1inUrRrfg1GP794QvZFFkW3/AlYddu1+oxmU0NtTzbglJG0dWhEKd6CRImVeWzWe
ZOmBO8+XvKtmXamaYmI9/+wexP5nXGccfeku0QWF/5/5+YrZwAmugQqY9Lp7B97+
CObkNf8=
-----END CERTIFICATE-----`
)

func TestCertificates_Create(t *testing.T) {
	c := qt.New(t)

	block, _ := pem.Decode([]byte(testPrivateKey))
	c.Assert(block, qt.Not(qt.IsNil), qt.Commentf("invalid PEM: "+testPrivateKey))

	pkey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	c.Assert(err, qt.IsNil)

	accessHost := "db.foo.example.com"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)

		var out = struct {
			Certificate      string         `json:"certificate"`
			CertificateChain string         `json:"certificate_chain"`
			AccessHost       string         `json:"access_host"`
			Ports            map[string]int `json:"ports"`
		}{
			Certificate:      testSignedPublicKey,
			CertificateChain: testCACert,
			AccessHost:       accessHost,
			Ports: map[string]int{
				"mysql-tls": 3306,
				"proxy":     3307,
			},
		}

		err := json.NewEncoder(w).Encode(out)
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	cert, err := client.Certificates.Create(ctx, &CreateCertificateRequest{
		Organization: "org-foo",
		DatabaseName: "db-foo",
		Branch:       "branch-foo",
		PrivateKey:   pkey,
	})
	c.Assert(err, qt.IsNil)

	c.Assert(cert.AccessHost, qt.Equals, accessHost)
	c.Assert(cert.CACerts, qt.HasLen, 1)
	c.Assert(cert.ClientCert, qt.Not(qt.IsNil))
	c.Assert(cert.ClientCert.PrivateKey, qt.Not(qt.IsNil))
	c.Assert(cert.ClientCert.Certificate, qt.HasLen, 1)

	ccert := cert.ClientCert.Certificate[0]
	ct, err := x509.ParseCertificate(ccert)
	c.Assert(err, qt.IsNil)
	c.Assert(ct.Subject.CommonName, qt.Equals, "org-foo/db-foo/branch-foo")
	c.Assert(ct.Issuer.CommonName, qt.Equals, cert.CACerts[0].Issuer.CommonName)

	c.Assert(cert.Ports.MySQL, qt.Equals, 3306)
	c.Assert(cert.Ports.Proxy, qt.Equals, 3307)
}
