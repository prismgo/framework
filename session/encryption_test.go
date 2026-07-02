package session

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type prefixEncryptor struct{}

func (prefixEncryptor) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	out := append([]byte("enc:"), plaintext...)
	for i := len("enc:"); i < len(out); i++ {
		out[i] = ^out[i]
	}
	return out, nil
}

func (prefixEncryptor) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	if !bytes.HasPrefix(ciphertext, []byte("enc:")) {
		return nil, errors.New("bad ciphertext with secret bytes")
	}
	out := append([]byte(nil), ciphertext[len("enc:"):]...)
	for i := range out {
		out[i] = ^out[i]
	}
	return out, nil
}

func TestFileDriverEncryptsPayloadWhenEnabled(t *testing.T) {
	cfg := testConfig(t)
	cfg.Encrypt = true
	cfg.Encryptor = prefixEncryptor{}
	driver := newTestFileDriver(t, cfg)
	id := newSessionID()

	expiresAt := testNow().Add(time.Hour)
	if err := driver.Write(context.Background(), id, Payload{
		ID:           id,
		Values:       map[string]any{"secret": "visible only after decrypt"},
		CreatedAt:    testNow(),
		LastActivity: testNow(),
	}, &expiresAt); err != nil {
		t.Fatalf("Write encrypted error = %v", err)
	}
	raw, err := os.ReadFile(driver.pathForID(id))
	if err != nil {
		t.Fatalf("ReadFile error = %v", err)
	}
	if !bytes.HasPrefix(raw, []byte("enc:")) || bytes.Contains(raw, []byte("visible only after decrypt")) {
		t.Fatalf("encrypted raw payload = %q", raw)
	}
	restored, err := driver.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read encrypted error = %v", err)
	}
	if restored.Values["secret"] != "visible only after decrypt" {
		t.Fatalf("restored secret = %#v", restored.Values["secret"])
	}
}

func TestFileDriverEncryptionFailuresAreSanitized(t *testing.T) {
	cfg := testConfig(t)
	cfg.Encrypt = true
	cfg.Encryptor = failingEncryptor{}
	driver := newTestFileDriver(t, cfg)
	id := newSessionID()

	expiresAt := testNow().Add(time.Hour)
	err := driver.Write(context.Background(), id, Payload{ID: id, Values: map[string]any{"secret": "payload"}}, &expiresAt)
	if !errors.Is(err, ErrEncryptionFailed) || bytes.Contains([]byte(err.Error()), []byte("secret")) {
		t.Fatalf("encryption error = %v", err)
	}

	cfg.Encryptor = prefixEncryptor{}
	driver = newTestFileDriver(t, cfg)
	if err := os.WriteFile(driver.pathForID(id), []byte("plain secret payload"), 0o600); err != nil {
		t.Fatalf("fixture write error = %v", err)
	}
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrDecryptionFailed) ||
		bytes.Contains([]byte(err.Error()), []byte("secret")) {
		t.Fatalf("decryption error = %v", err)
	}
}
