package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func sign(method, url, body string, hdr map[string]string, token string) {
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if body == "" {
		req, _ = http.NewRequest(method, url, nil)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	h := sha256.Sum256([]byte(body))
	creds := aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", SessionToken: token}
	err := signer.NewSigner().SignHTTP(context.Background(), creds, req, hex.EncodeToString(h[:]), "aps", "us-west-2", time.Unix(1700000000, 0).UTC())
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s\n  Authorization: %s\n  X-Amz-Date: %s\n  X-Amz-Security-Token: %s\n  Host: %s\n", method, url, req.Header.Get("Authorization"), req.Header.Get("X-Amz-Date"), req.Header.Get("X-Amz-Security-Token"), req.Host)
}

func main() {
	sign("POST", "https://aps-workspaces.us-west-2.amazonaws.com/workspaces/ws-1/api/v1/remote_write", "hello snappy", map[string]string{"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy", "X-Prometheus-Remote-Write-Version": "0.1.0", "User-Agent": "Prometheus/3.16.0"}, "")
	sign("GET", "https://example.com:443/a%20b/c?b=2&a=1&a=0&z=x+y", "", map[string]string{"X-Multi": "  a   b  "}, "TOKEN")
	sign("GET", "http://example.com:8080/", "", nil, "")
}
