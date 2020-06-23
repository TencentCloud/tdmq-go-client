package authcloud

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestAuthenticationCloudCam_Initialize(t *testing.T) {
	authCloudCam := NewDefaultAuthenticationCloudCam()
	authParams := make(map[string]string)
	authParams["secretId"] = "xxxxxxxxxx"
	authParams["secretKey"] = "xxxxxxxxxx"
	authParams["region"] = "ap-guangzhou"
	authParams["apiUrl"] = "http://xx.xx.xx.xx:xxxx"
	//authParams["apiUrl"] = "http://localhost:8080"
	err := authCloudCam.Initialize(authParams)
	if err != nil {
		t.Error(err)
	}
	t.Log("\nOwnerUin : " + authCloudCam.(*AuthenticationCloudCam).ownerUin + ";\n" +
		"uin : " + authCloudCam.(*AuthenticationCloudCam).uin + ";\n" +
		"appId : " + authCloudCam.(*AuthenticationCloudCam).appId + ";\n")
}

func TestAuthenticationCloudCam_CreateAuthMetadata(t *testing.T) {
	authCloudCam := NewDefaultAuthenticationCloudCam()
	authParams := make(map[string]string)
	authParams["secretId"] = "xxxxxxxxxx"
	authParams["secretKey"] = "xxxxxxxxxx"
	authParams["region"] = "ap-guangzhou"
	authParams["apiUrl"] = "http://xx.xx.xx.xx:xxxx"
	//authParams["apiUrl"] = "http://localhost:8080"
	err := authCloudCam.Initialize(authParams)
	if err != nil {
		t.Error(err)
	}
	metaMap := make(map[string]string)
	authCloudCam.CreateAuthMetadata("connect", metaMap)
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	err = jsonEncoder.Encode(metaMap)
	if err == nil {
		metaJsonStr := bf.String()
		t.Log(metaJsonStr)
	}
}
