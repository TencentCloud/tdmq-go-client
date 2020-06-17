package authcloud

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"strings"
	"testing"
)

func TestAuth(t *testing.T) {

}

func TestSig(t *testing.T) {

}

func TestGetSignKey(t *testing.T) {
	params := make(map[string]interface{})
	params["aciotn"] = "connect"
	sigUrl := "./initialize"
	signKey := GetSignKey(params, sigUrl, "SBMEhSrlABZb30UR00XfDP5Ev7yEzRCb")
	assert.Equal(t, strings.ToLower("2467ad53dfcd5ad4c3b7f1699c799e2475ed418c"), signKey)
}

func TestGetSignKey2(t *testing.T) {
	metadata := make(map[string]string)
	sigUrl := "connect"
	//timeStamp := time.Now().UnixNano() / 1e6
	sigParams := make(map[string]interface{})
	sigParams["aciotn"] = "./" + "connect"
	sigParams["timesTamp"] = "1591155744486"
	if v, ok := metadata["topic"]; ok {
		sigParams["topic"] = v
	} else {
		sigParams["topic"] = "null" // for java null String
	}
	if v, ok := metadata["requestId"]; ok {
		sigParams["requestId"] = v
	} else {
		sigParams["requestId"] = "null" // for java null String
	}
	if v, ok := metadata["clientId"]; ok {
		sigParams["clientId"] = v
	} else {
		sigParams["clientId"] = "null" //for java null String
	}
	for k, v := range sigParams {
		t.Log(k + ":" + fmt.Sprintln(v) + "\n")
	}
	signStr := GetSignKey(sigParams, sigUrl, "SBMEhSrlABZb30UR00XfDP5Ev7yEzRCb")
	t.Log("Sign Str : " + signStr)
	assert.Equal(t, "d71922228ec8e2b5d71b4155178e8268c26801c6", signStr)
}

func TestGetSignSrc(t *testing.T) {
	metadata := make(map[string]string)
	sigUrl := "connect"
	//timeStamp := time.Now().UnixNano() / 1e6
	sigParams := make(map[string]interface{})
	sigParams["aciotn"] = "./" + "connect"
	sigParams["timesTamp"] = "1591155744486"
	if v, ok := metadata["topic"]; ok {
		sigParams["topic"] = v
	} else {
		sigParams["topic"] = "null" // for java null String
	}
	if v, ok := metadata["requestId"]; ok {
		sigParams["requestId"] = v
	} else {
		sigParams["requestId"] = "null" // for java null String
	}
	if v, ok := metadata["clientId"]; ok {
		sigParams["clientId"] = v
	} else {
		sigParams["clientId"] = "null" //for java null String
	}
	for k, v := range sigParams {
		t.Log(k + ":" + fmt.Sprintln(v) + "\n")
	}
	keys := make([]string, len(sigParams))
	for k, _ := range sigParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var str string
	for _, key := range keys {
		str = str + key + "=" + fmt.Sprint(sigParams[key]) + "&"
	}
	assert.Equal(t, "POSTconnect?aciotn=./connect&clientId=null&requestId=null&timesTamp=1591155744486&topic=null", GetSignSrc(sigParams, sigUrl))
}

func TestMap2Str(t *testing.T) {

}
