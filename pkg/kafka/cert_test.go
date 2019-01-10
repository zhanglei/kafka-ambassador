package kafka

import (
	"encoding/pem"
	"io/ioutil"
	"testing"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestCertExpirationTime(t *testing.T) {
	key := helperGenerateKey(t)
	caKey := helperGenerateKey(t)

	nva := time.Now().Add(1 * time.Hour)
	ca := helperGenerateCA(t, key, nva)

	certDer := helperGenerateClientCert(t, key, ca, caKey, nva)

	certPath := "/tmp/test.cert"

	pemCert := pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certDer,
		},
	)
	ioutil.WriteFile(certPath, pemCert, 0700)
	expirationTime, err := CertExpirationTime(certPath)
	assert.NoError(t, err, "Could not get expiration time")
	assert.WithinDuration(t, nva, expirationTime, 1*time.Minute, "Expiration time should match")

	configMap := make(kafka.ConfigMap)
	flatten := config.Flatten(configMap)
	configPath := "ssl.certificate.location"
	flatten[configPath] = certPath

	for k, v := range flatten {
		configMap[k] = v
	}

	pExpirationTime, err := ParamsCertExpirationTime(&configMap, configPath)
	assert.NoError(t, err, "Could not get expiration time")
	assert.WithinDuration(t, nva, pExpirationTime, 1*time.Minute, "Expiration time should match")
}
