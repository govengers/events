package postgrestest

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
)

// Sealer is an events.Sealer for tests: AES-256-GCM with a fixed key, the associated data
// authenticated like the seal package of govengers/auth does.
type Sealer struct{}

var sealerKey = []byte("0123456789abcdef0123456789abcdef")

func (Sealer) aead() cipher.AEAD {
	block, err := aes.NewCipher(sealerKey)
	if err != nil {
		panic(err) // a 32-byte key never fails
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return aead
}

// Seal encrypts plaintext bound to associatedData.
func (s Sealer) Seal(plaintext, associatedData []byte) ([]byte, error) {
	aead := s.aead()
	nonce := make([]byte, aead.NonceSize())
	_, _ = rand.Read(nonce)
	return aead.Seal(nonce, nonce, plaintext, associatedData), nil
}

// Open decrypts what Seal returned for the same associatedData.
func (s Sealer) Open(sealed, associatedData []byte) ([]byte, error) {
	aead := s.aead()
	if len(sealed) < aead.NonceSize() {
		return nil, errors.New("postgrestest: sealed value too short")
	}
	return aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], associatedData)
}
