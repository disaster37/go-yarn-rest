package client

import (
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/jarcoal/httpmock"
	"time"
)

func TestApplications(t *testing.T) {

	logrus.SetLevel(logrus.DebugLevel)
	
	client := New("http://fake.local", "", "")
	httpmock.ActivateNonDefault(client.Client().GetClient())
	
	// When no filters
	fixture := `{"apps":{"app":[{"id":"application_1541145585648_0114","user":"user1","name":"Job.Name","queue":"default","state":"FAILED","finalStatus":"FAILED","trackingUI":"History","trackingUrl":"http://yarn:8088/cluster/app/application_1541145585648_0114","applicationType":"SPARK","startedTime":1541432272828,"finishedTime":1541434233846,"diagnostics":"there are some problems"}]}}`
	responder := httpmock.NewStringResponder(200, fixture)
	fakeUrl := "http://fake.local/cluster/apps"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	jobs, err := client.Applications(nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, "application_1541145585648_0114", jobs[0].Id)
	assert.Equal(t, "user1", jobs[0].User)
	assert.Equal(t, "Job.Name", jobs[0].Name)
	assert.Equal(t, "default", jobs[0].Queue)
	assert.Equal(t, "FAILED", jobs[0].State)
	assert.Equal(t, "FAILED", jobs[0].FinalStatus)
	assert.Equal(t, "History", jobs[0].TrackingUI)
	assert.Equal(t, "http://yarn:8088/cluster/app/application_1541145585648_0114", jobs[0].TrackingUrl)
	assert.Equal(t, "SPARK", jobs[0].ApplicationType)
	assert.Equal(t, time.Unix(0, 1541432272828000000), jobs[0].StartedDateTime())
	assert.Equal(t, time.Unix(0, 1541434233846000000), jobs[0].FinishedDateTime())
	assert.Equal(t, "there are some problems", jobs[0].Diagnostics)
	
	// When use filter
	filters := map[string]string {
	    "user": "user1",
	    "queue": "default",
	    "finishedTimeBegin": "1541432272828000",
	    "states": "FAILED",
	}
	httpmock.Reset()
	responder = httpmock.NewStringResponder(200, `{"apps":{"app":null}}`)
	httpmock.RegisterResponder("GET", fakeUrl+"?finishedTimeBegin=1541432272828000&queue=default&states=FAILED&user=user1", responder)
	jobs, err = client.Applications(filters)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	

}
