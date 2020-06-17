package authcloud

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestAuthenticationCloudCam_Initialize(t *testing.T) {
	authCloudCam := NewDefaultAuthenticationCloudCam()
	authParams := make(map[string]string)
	authParams["secretId"] = "AKID35cjINI6m3zI4PrbUpnF9giEFkkMg5Cx"
	authParams["secretKey"] = "SBMEhSrlABZb30UR00XfDP5Ev7yEzRCb"
	authParams["region"] = "ap-guangzhou"
	authParams["apiUrl"] = "http://10.104.62.35:9502"
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
	authParams["secretId"] = "AKID35cjINI6m3zI4PrbUpnF9giEFkkMg5Cx"
	authParams["secretKey"] = "SBMEhSrlABZb30UR00XfDP5Ev7yEzRCb"
	authParams["region"] = "ap-guangzhou"
	authParams["apiUrl"] = "http://10.104.62.35:9502"
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
