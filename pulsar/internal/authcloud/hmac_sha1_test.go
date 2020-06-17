package authcloud

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestSign(t *testing.T) {
	expectStr := strings.ToLower("D207BF2EABFED4E42D64B755DBC0FEC4431D04D7")
	secretKey := "ov48wmzacuu4dejg"
	str := "YXBwa2V5PTlSMjkxVVNFMDQmcmFuZG9tPTQ2ODEwJnRpbWU9MTUwNzUzMzQ3NDcwMCZ2ZXJzaW9uPTEuMA=="
	resStr := HmacSHA1Sign(secretKey, str)
	assert.Equal(t, expectStr, resStr)
}
