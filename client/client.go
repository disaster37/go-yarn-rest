// This file permit to manage the Yarn ResourceManager client API

package client

import (
	"crypto/tls"
	"github.com/go-resty/resty"
)

// Yarn client object
type YarnClient struct {
	client *resty.Client
}

// New permit to create new Yarn client
// It return AmbariClient
func New(baseUrl string, login string, password string) *YarnClient {
	client := &YarnClient{
		client: resty.New().SetHostURL(baseUrl),
	}

	if login != "" && password != "" {
		client.SetBasicAuth(login, password)
	}

	return client
}

// Pertmit to set custom resty.Client for advance option
func (c *YarnClient) SetClient(client *resty.Client) {

	if client == nil {
		panic("Client can't be empty")
	}

	c.client = client
}

// Client permit to return resty.Client Object
func (c *YarnClient) Client() *resty.Client {
	return c.client
}

// DisableVerifySSL permit to disable the SSL certificat check when call Ambari webservice
func (c *YarnClient) DisableVerifySSL() {
	c.client = c.client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
}
