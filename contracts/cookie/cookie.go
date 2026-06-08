// Package cookie 定义 Cookie 安全扩展的公共契约。
//
// 本包只声明 Cookie 签名接口；Cookie 加密统一使用 contracts/encryption.StringEncrypter。
// Cookie 值对象、队列和 middleware 实现由 prismgo/cookie 实现包提供。
package cookie

import "context"

// Signer 是 Cookie 签名和验签的契约。
//
// 用途：对即将写入浏览器的 Cookie 值附加防篡改签名，请求读取时验证签名完整性。
//
// 使用方式：自定义实现在 Attach 时通过 WithSigner 注入，或通过全局配置默认签名器。
//
//	type HMACSigner struct { key []byte }
//	func (s *HMACSigner) Sign(ctx context.Context, name, value string) (string, error) {
//	    mac := hmac.New(sha256.New, s.key)
//	    mac.Write([]byte(name + value))
//	    return hex.EncodeToString(mac.Sum(nil)), nil
//	}
type Signer interface {
	// Sign 对 Cookie 值进行签名。
	//
	// 参数 name 是 Cookie 名称，可用于派生签名密钥。
	// 参数 value 是待签名的原始值。
	// 返回签名后的值；错误不能包含原始 Cookie 值。
	Sign(ctx context.Context, name string, value string) (string, error)

	// Unsign 验证并剥离签名。
	//
	// 参数 name 是 Cookie 名称。
	// 参数 value 是请求中收到的含签名值。
	// 返回验签后的原始值；签名无效时返回错误。
	Unsign(ctx context.Context, name string, value string) (string, error)
}
