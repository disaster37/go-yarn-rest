// Yarn documentation: http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Application_API
// This file permit to manage Application in Yarn API

package client

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

type Applications struct {
	Applications *Application `json:"apps"`
}

type Application struct {
	ApplicationInfos []ApplicationInfo `json:"app"`
}

type ApplicationInfo struct {
	Id              string  `json:"id,omitempty"`
	User            string  `json:"user,omitempty"`
	Name            string  `json:"name,omitempty"`
	Queue           string  `json:"queue,omitempty"`
	State           string  `json:"state,omitempty"`
	FinalStatus     string  `json:"finalStatus,omitempty"`
	Progess         float64 `json:"progress,omitempty"`
	TrackingUI      string  `json:"trackingUI,omitempty"`
	TrackingUrl     string  `json:"trackingUrl,omitempty"`
	ApplicationType string  `json:"applicationType,omitempty"`
	StartedTime     int64   `json:"startedTime,omitempty"`
	FinishedTime    int64   `json:"finishedTime,omitempty"`
	Diagnostics     string  `json:"diagnostics,omitempty"`
}

// String permit to return Application as Json string
func (a *ApplicationInfo) String() string {
	json, _ := json.Marshal(a)
	return string(json)
}

//String permit to return Applications as Json string
func (a *Applications) String() string {
	json, _ := json.Marshal(a)
	return string(json)
}

// StartedDateTime return StartedTime as time.Time
func (a *ApplicationInfo) StartedDateTime() time.Time {
	return time.Unix(0, a.StartedTime*1000000)
}

// FinishedDateTime return FinishedTime as time.Time
func (a *ApplicationInfo) FinishedDateTime() time.Time {
	return time.Unix(0, a.FinishedTime*1000000)
}

// Applications permit to get all application that match the given filters
// It return the list of Application if found
// It return empty list if not found
// It return error if something wrong when API call
func (c *YarnClient) Applications(filters map[string]string) ([]ApplicationInfo, error) {

	log.Debug("Filters: ", filters)

	path := fmt.Sprintf("cluster/apps")
	resp, err := c.Client().R().SetQueryParams(filters).Get(path)
	if err != nil {
		return nil, err
	}
	log.Debug("Response to get: ", resp)
	if resp.StatusCode() >= 300 {
		if resp.StatusCode() == 404 {
			return nil, nil
		} else {
			return nil, NewYarnError(resp.StatusCode(), resp.Status())
		}
	}
	applications := &Applications{}
	err = json.Unmarshal(resp.Body(), applications)
	if err != nil {
		return nil, err
	}
	log.Debugf("Return applications: %s", applications)

	return applications.Applications.ApplicationInfos, nil
}
