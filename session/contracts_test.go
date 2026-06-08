package session

import sessioncontract "github.com/prismgo/framework/contracts/session"
import encryptioncontract "github.com/prismgo/framework/contracts/encryption"

var (
	_ Payload                      = sessioncontract.Payload{}
	_ sessioncontract.Payload      = Payload{}
	_ Driver                       = sessioncontract.Driver(nil)
	_ sessioncontract.Driver       = Driver(nil)
	_ Locker                       = sessioncontract.Locker(nil)
	_ sessioncontract.Locker       = Locker(nil)
	_ Lock                         = sessioncontract.Lock(nil)
	_ sessioncontract.Lock         = Lock(nil)
	_ Encryptor                    = encryptioncontract.Encrypter(nil)
	_ encryptioncontract.Encrypter = Encryptor(nil)
)
