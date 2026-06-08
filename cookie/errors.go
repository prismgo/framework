package cookie

import "errors"

var (
	// ErrInvalidCookieName 表示 cookie 名称为空或不符合 HTTP token 规则。
	ErrInvalidCookieName = errors.New("cookie: invalid name")
	// ErrCookieNotFound 表示请求中不存在指定 cookie。
	ErrCookieNotFound = errors.New("cookie: not found")
	// ErrQueueNotFound 表示当前 Gin 请求没有安装请求级 cookie 队列。
	ErrQueueNotFound = errors.New("cookie: request queue not found")
	// ErrCookieSignature 表示签名校验失败，通常说明客户端值被篡改或密钥不匹配。
	ErrCookieSignature = errors.New("cookie: signature invalid")
	// ErrCookieEncryption 表示写出 cookie 前加密失败。
	ErrCookieEncryption = errors.New("cookie: encryption failed")
	// ErrCookieDecryption 表示读取 cookie 时解密失败。
	ErrCookieDecryption = errors.New("cookie: decryption failed")
)

// SensitiveError 表示涉及原始 cookie 值的安全失败。
//
// 需求背景：cookie 值来自客户端，可能包含签名、密文或用户输入，错误文本不应回显原始值。
// 设计思路：Error 只描述操作失败，Unwrap 保留可识别的错误哨兵，后续测试可以验证错误
// 类型而不依赖敏感字符串。
type SensitiveError struct {
	// Op 是失败的逻辑操作名称，例如 verify value 或 decrypt value。
	Op string
	// Err 是可被 errors.Is/As 识别的底层错误哨兵。
	Err error
}

// Error 返回脱敏后的错误文本。
func (e SensitiveError) Error() string {
	if e.Op == "" {
		return "cookie: sensitive operation failed"
	}
	return "cookie: " + e.Op + " failed"
}

// Unwrap 返回底层错误，便于调用方执行显式错误判断。
func (e SensitiveError) Unwrap() error {
	return e.Err
}

// safeError 把签名、加密、解密等敏感操作统一包装成脱敏错误。
//
// 参数 op 表示当前失败的安全操作；参数 err 表示稳定的错误分类。err 为空时返回 nil，
// 方便调用方保持线性错误处理。
func safeError(op string, err error) error {
	if err == nil {
		return nil
	}
	return SensitiveError{Op: op, Err: err}
}
