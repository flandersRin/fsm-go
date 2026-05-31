//go:build integration

package integration_test

import (
	"testing"

	"github.com/flandersrin/workflow-go/messaging/kafka"
	"github.com/flandersrin/workflow-go/persistence/mysql"
	"github.com/flandersrin/workflow-go/persistence/postgres"
	"github.com/flandersrin/workflow-go/workflow"
	"github.com/flandersrin/workflow-go/workflowtest"
)

func TestDefaultAdaptersExposeWorkflowInterfaces(t *testing.T) {
	var _ workflow.Store = workflowtest.NewMemoryStore()
	if mysql.Schema == "" {
		t.Fatal("mysql schema is empty")
	}
	if postgres.Schema == "" {
		t.Fatal("postgres schema is empty")
	}
	publisher := kafka.NewOutboxPublisher(nil)
	if publisher == nil {
		t.Fatal("expected kafka outbox publisher")
	}
}
