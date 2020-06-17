package authcloud

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"time"
)

func DoPost(
	url string,
	param string,
	connectTimeOut int,
	readTimeOut int) ([]byte, error) {
	httpClient := &http.Client{
		Timeout: time.Duration(connectTimeOut+readTimeOut) * time.Millisecond,
	}
	request, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(param)))
	if err != nil {
		return []byte{}, err
	}

	request.Header.Add("accept", "*/*")
	request.Header.Add("connection", "Keep-Alive")
	request.Header.Add("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)")
	request.Header.Add("content-type", "application/x-www-form-urlencoded")

	response, err := httpClient.Do(request)
	if err != nil {
		return []byte{}, err
	}
	defer response.Body.Close()

	resBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return []byte{}, nil
	}
	return resBytes, nil
}
