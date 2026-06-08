package cookie

import cookiecontract "github.com/prismgo/framework/contracts/cookie"
import encryptioncontract "github.com/prismgo/framework/contracts/encryption"

var (
	_ Signer                             = cookiecontract.Signer(nil)
	_ cookiecontract.Signer              = Signer(nil)
	_ Encryptor                          = encryptioncontract.StringEncrypter(nil)
	_ encryptioncontract.StringEncrypter = Encryptor(nil)
)
