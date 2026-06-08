package encryption

import (
	"bytes"
	"context"
	"testing"

	"github.com/prismgo/framework/container"
	contractsencryption "github.com/prismgo/framework/contracts/encryption"
)

// TestFacadeResolveReturnsContractInterfaces 验证 facade 解析的是 contracts/encryption 契约类型。
func TestFacadeResolveReturnsContractInterfaces(t *testing.T) {
	registry := useEncryptionFacadeRegistry(t)
	fake := &recordingEncrypter{}
	if err := registry.Instance(serviceKey, contractsencryption.Encrypter(fake)); err != nil {
		t.Fatalf("bind encrypter contract: %v", err)
	}

	resolved := Resolve()
	if resolved != fake {
		t.Fatal("Resolve should return the registered Encrypter contract")
	}
}

// TestFacadeResolveStringReturnsContractInterface 验证字符串 facade 使用字符串加密契约解析同一服务。
func TestFacadeResolveStringReturnsContractInterface(t *testing.T) {
	registry := useEncryptionFacadeRegistry(t)
	fake := &recordingEncrypter{}
	if err := registry.Instance(serviceKey, contractsencryption.StringEncrypter(fake)); err != nil {
		t.Fatalf("bind string encrypter contract: %v", err)
	}

	resolved := ResolveString()
	if resolved != fake {
		t.Fatal("ResolveString should return the registered StringEncrypter contract")
	}
}

// TestFacadeHelpersDelegateThroughContracts 验证 helper 不绕过契约，全部转发给当前容器服务。
func TestFacadeHelpersDelegateThroughContracts(t *testing.T) {
	registry := useEncryptionFacadeRegistry(t)
	fake := &recordingEncrypter{}
	if err := registry.Instance(serviceKey, fake); err != nil {
		t.Fatalf("bind encrypter: %v", err)
	}

	ctx := context.Background()
	if out, err := Encrypt(ctx, []byte("plain")); err != nil || !bytes.Equal(out, []byte("encrypted:plain")) {
		t.Fatalf("Encrypt = %q err=%v", out, err)
	}
	if out, err := Decrypt(ctx, []byte("encrypted:plain")); err != nil || !bytes.Equal(out, []byte("plain")) {
		t.Fatalf("Decrypt = %q err=%v", out, err)
	}
	if out, err := EncryptString(ctx, "text"); err != nil || out != "encrypted:text" {
		t.Fatalf("EncryptString = %q err=%v", out, err)
	}
	if out, err := DecryptString(ctx, "encrypted:text"); err != nil || out != "text" {
		t.Fatalf("DecryptString = %q err=%v", out, err)
	}
	if fake.encryptCalls != 1 || fake.decryptCalls != 1 || fake.encryptStringCalls != 1 || fake.decryptStringCalls != 1 {
		t.Fatalf("delegation counts = %#v", fake)
	}
}

func useEncryptionFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

type recordingEncrypter struct {
	encryptCalls       int
	decryptCalls       int
	encryptStringCalls int
	decryptStringCalls int
}

func (e *recordingEncrypter) Encrypt(context.Context, []byte) ([]byte, error) {
	e.encryptCalls++
	return []byte("encrypted:plain"), nil
}

func (e *recordingEncrypter) Decrypt(context.Context, []byte) ([]byte, error) {
	e.decryptCalls++
	return []byte("plain"), nil
}

func (e *recordingEncrypter) EncryptString(context.Context, string) (string, error) {
	e.encryptStringCalls++
	return "encrypted:text", nil
}

func (e *recordingEncrypter) DecryptString(context.Context, string) (string, error) {
	e.decryptStringCalls++
	return "text", nil
}

var (
	_ contractsencryption.Encrypter       = (*recordingEncrypter)(nil)
	_ contractsencryption.StringEncrypter = (*recordingEncrypter)(nil)
)
