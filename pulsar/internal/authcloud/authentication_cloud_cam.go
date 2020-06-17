package authcloud

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

type AuthenticationCloud interface {
	GetAuthMethodName() string

	Initialize(authParams map[string]string) error

	CreateAuthMetadata(action string, metadata map[string]string)

	io.Closer
}

const AUTHMETHODNAME = "CAM"

type AuthenticationCloudCam struct {
	secretId          string
	secretKey         string
	apiUrl            string
	region            string
	ownerUin          string
	uin               string
	appId             string
	connectionTimeOut int
	readTimeOut       int
}

func NewDefaultAuthenticationCloudCam() AuthenticationCloud {
	return &AuthenticationCloudCam{
		connectionTimeOut: 10000,
		readTimeOut:       10000,
	}
}

func (authCam *AuthenticationCloudCam) Close() error {
	return nil
}

func (authCam *AuthenticationCloudCam) GetAuthMethodName() string {
	return AUTHMETHODNAME
}

func (authCam *AuthenticationCloudCam) Initialize(authParams map[string]string) error {
	authCam.secretId = authParams["secretId"]
	authCam.secretKey = authParams["secretKey"]
	//authCam.apiUrl = authParams["apiUrl"]
	authCam.region = authParams["region"]
	authCam.ownerUin = authParams["ownerUin"]
	authCam.uin = authParams["uin"]

	if v, ok := authParams["connectTimeout"]; ok {
		iv, _ := strconv.Atoi(v)
		authCam.connectionTimeOut = iv
	}
	if v, ok := authParams["readTimeOut"]; ok {
		iv, _ := strconv.Atoi(v)
		authCam.readTimeOut = iv
	}
	if len(authCam.secretId) == 0 {
		return fmt.Errorf("secretId is illegal")
	}
	if len(authCam.secretKey) == 0 {
		return fmt.Errorf("secretKey is illegal")
	}
	//if len(authCam.apiUrl) == 0 {
	//	return fmt.Errorf("apiUrl is illegal")
	//}
	if len(authCam.region) == 0 {
		return fmt.Errorf("region is illegal")
	}
	if len(authCam.ownerUin) == 0 {
		return fmt.Errorf("ownerUin is illegal")
	}
	if len(authCam.uin) == 0 {
		return fmt.Errorf("uin is illegal")
	}
	//resMap := authCam.sig()
	//
	//if resMap == nil || len(resMap) == 0 {
	//	return fmt.Errorf("Http Response Map is nil or empty! ")
	//}
	//
	//if int(resMap["returnCode"].(float64)) == 0 && int(resMap["returnValue"].(float64)) == 0 {
	//	dataMap := resMap["data"].(map[string]interface{})
	//	authCam.ownerUin = dataMap["ownerUinStr"].(string)
	//	authCam.uin = dataMap["uinStr"].(string)
	//	authCam.appId = dataMap["ownerAppidStr"].(string)
	//}
	return nil
}

func (authCam *AuthenticationCloudCam) sig() map[string]interface{} {
	params := make(map[string]interface{})
	params["aciotn"] = "connect"
	sigUrl := "./initialize"
	signSrc := GetSignSrc(params, sigUrl)
	signStr := GetSignKey(params, sigUrl, authCam.secretKey)
	return Sig(authCam.apiUrl, authCam.secretId, "connect",
		signStr, signSrc, authCam.connectionTimeOut, authCam.readTimeOut)
}

func (authCam *AuthenticationCloudCam) CreateAuthMetadata(action string, metadata map[string]string) {
	sigUrl := action
	timeStamp := time.Now().UnixNano() / 1e6
	sigParams := make(map[string]interface{})
	sigParams["aciotn"] = "./" + action
	sigParams["timesTamp"] = strconv.FormatInt(timeStamp, 10)
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
	signStr := GetSignKey(sigParams, sigUrl, authCam.secretKey)
	delete(metadata, "requestId")
	delete(metadata, "producerId")
	metadata["aciotn"] = action
	metadata["secretId"] = authCam.secretId
	metadata["timesTamp"] = strconv.FormatInt(timeStamp, 10)
	metadata["sign"] = signStr
	metadata["region"] = authCam.region
	metadata["ownerUin"] = authCam.ownerUin
	metadata["uin"] = authCam.uin
}
