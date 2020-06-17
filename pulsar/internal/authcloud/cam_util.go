package authcloud

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ALGORITHM                = "HmacSHA1"
	METHOD                   = "POST"
	SIGANDAUTH_INTERFACENAME = "logic.cam.sigAndAuth"
	SIGNMODE                 = 1
	AUTHMODE                 = 2
)

func SigAndAuth(
	apiIp string,
	secretId string,
	action string,
	signStr string,
	signSrc string,
	resource string,
	connectTimeOut int,
	readTimeOut int) (map[string]interface{}, error) {
	sigMap := Sig(apiIp, secretId, action, signStr, signSrc, connectTimeOut, readTimeOut)
	if v, err := strconv.Atoi(fmt.Sprint(sigMap["returnCode"])); err == nil && v == 0 {
		dataMap := sigMap["data"].(map[string]interface{})
		ownerUin := dataMap["ownerUinStr"].(string)
		uin := dataMap["uinStr"].(string)
		return Auth(apiIp, ownerUin, uin, action, resource, connectTimeOut, readTimeOut)
	} else {
		return sigMap, err
	}
}

func Auth(
	apiIp string,
	ownerUin string,
	uin string,
	action string,
	resource string,
	connectTimeOut int,
	readTimeOut int) (map[string]interface{}, error) {
	params := make(map[string]interface{})
	params["version"] = "0"
	params["eventId"] = GetUUID32()
	params["timestamp"] = fmt.Sprintf("%d", time.Now().Unix())
	interfaceMap := make(map[string]interface{})
	interfaceMap["interfaceName"] = SIGANDAUTH_INTERFACENAME
	para := make(map[string]interface{})
	para["version"] = "2.0"
	para["uin"] = uin
	para["ownerUin"] = ownerUin
	para["action"] = action
	para["mod"] = AUTHMODE
	var resourceList []string
	resourceList = append(resourceList, resource)
	para["resource"] = resourceList
	interfaceMap["para"] = para
	params["interface"] = interfaceMap

	paramsJsonBytes, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	resBytes, err := DoPost(apiIp, string(paramsJsonBytes), connectTimeOut, readTimeOut)
	var resultMap map[string]interface{}
	err = json.Unmarshal(resBytes, &resultMap)
	if err != nil {
		return nil, err
	}
	return resultMap, nil
}

func Sig(
	apiIp string,
	secretId string,
	action string,
	signStr string,
	signSrc string,
	connectTimeout int,
	readTimeout int) map[string]interface{} {
	params := make(map[string]interface{})
	params["version"] = "0"
	params["eventId"] = GetUUID32()
	params["timestamp"] = fmt.Sprintf("%d", time.Now().Unix())
	interfaceMap := make(map[string]interface{})
	interfaceMap["interfaceName"] = SIGANDAUTH_INTERFACENAME
	para := make(map[string]interface{})
	para["version"] = "2.0"
	para["url"] = "/"
	para["action"] = action
	para["mode"] = SIGNMODE

	var authorization string
	authorization = authorization + "authorization:" + "q-sign-algorithm=cdp_sha1" + "&" +
		"q-plaintext=" + base64.StdEncoding.EncodeToString([]byte(signSrc)) + "&" +
		"q-ak=" + secretId + "&" +
		"q-sign-time=" + fmt.Sprintf("%d", time.Now().Unix()-10) + ";" + fmt.Sprintf("%d", time.Now().Unix()+10) + "&" +
		"q-key-time=" + fmt.Sprintf("%d", time.Now().Unix()-10) + ";" + fmt.Sprintf("%d", time.Now().Unix()+10) + "&" +
		"q-url-param-list=" + "&" +
		"q-header-list=" + "&" +
		"q-signature=" + signStr

	para["header"] = authorization
	interfaceMap["para"] = para
	params["interface"] = interfaceMap
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	err := jsonEncoder.Encode(params)
	if err != nil {
		return make(map[string]interface{})
	}
	resBytes, err := DoPost(apiIp, bf.String(), connectTimeout, readTimeout)
	if err != nil {
		return make(map[string]interface{})
	}
	resultMap := make(map[string]interface{})
	err = json.Unmarshal(resBytes, &resultMap)
	if err != nil {
		return make(map[string]interface{})
	}
	return resultMap
}

func GetSignKey(
	params map[string]interface{},
	sigurl string,
	secretKey string) string {
	//var str string
	//str = str + METHOD + sigurl + "?"
	//for paramKey, value := range params {
	//	str = str + paramKey + "=" + fmt.Sprint(value) + "&"
	//}
	srcStr := GetSignSrc(params, sigurl)
	if srcStr == "" {
		return ""
	}
	return HmacSHA1Sign(secretKey, srcStr)
}

func GetSignSrc(
	params map[string]interface{},
	sigurl string) string {
	if sigurl == "" || params == nil || len(params) == 0 {
		return ""
	}
	var str string
	str = METHOD + sigurl + "?"
	map2Str := Map2Str(params)
	if map2Str == "" {
		return ""
	}
	str += map2Str
	return str
}

//for tree map (key sorted by string) tans to string
func Map2Str(params map[string]interface{}) string {
	if params == nil || len(params) == 0 {
		return ""
	}
	keys := make([]string, 0, len(params))
	for k, _ := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var str string
	for _, key := range keys {
		str = str + key + "=" + fmt.Sprint(params[key]) + "&"
	}
	return str[:len(str)-1]
}

func GetUUID32() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}
