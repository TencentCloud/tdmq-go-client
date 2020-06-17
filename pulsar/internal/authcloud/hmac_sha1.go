package authcloud

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"strings"
)

func HmacSHA1Sign(secretKey string, src string) string {
	key := []byte(secretKey)
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(src))
	return strings.ToLower(hex.EncodeToString(mac.Sum(nil)))
}
