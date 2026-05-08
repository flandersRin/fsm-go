//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/flandersrin/fsm-go/actions"
	"github.com/flandersrin/fsm-go/fsm"
	mysqlrepo "github.com/flandersrin/fsm-go/persistence/mysql"
)

func TestMySQLRepositoryEndToEndWithRealDependencies(t *testing.T) {
	ctx := context.Background()
	db := startMySQL(t, ctx)
	repo := mysqlrepo.NewRepository(db)
	if err := repo.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	runtime := newRuntime(t, repo)
	if err := runtime.CreateEntity(ctx, fsm.StateEntity{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-it-1",
		State:          "PENDING",
		Data:           map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}

	cmd := fsm.FireCommand{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-it-1",
		Event:          "PAY_SUCCESS",
		Actor:          fsm.Actor{ID: "user-1", Role: "customer"},
		RequestID:      "req-1",
		IdempotencyKey: "idem-1",
		Payload:        map[string]any{"paymentStatus": "SUCCESS", "amount": 100},
	}
	result, err := runtime.Fire(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if result.ToState != "PAID" {
		t.Fatalf("expected PAID, got %s", result.ToState)
	}

	again, err := runtime.Fire(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !again.IdempotentHit {
		t.Fatal("expected idempotent hit")
	}

	var state string
	if err := db.QueryRowContext(ctx, "SELECT state FROM fsm_entity WHERE machine = 'order' AND entity_id = 'order-it-1'").Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "PAID" {
		t.Fatalf("expected persisted PAID, got %s", state)
	}
	assertCount(t, db, "fsm_state_log", 1)
	assertCount(t, db, "fsm_outbox", 1)
	assertCount(t, db, "fsm_idempotency", 1)
}

func TestMySQLRepositoryConcurrentTransitionAllowsOnlyOneWinner(t *testing.T) {
	ctx := context.Background()
	db := startMySQL(t, ctx)
	repo := mysqlrepo.NewRepository(db)
	if err := repo.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	runtime := newRuntime(t, repo)
	if err := runtime.CreateEntity(ctx, fsm.StateEntity{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-race-1",
		State:          "PENDING",
		Data:           map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := runtime.Fire(ctx, fsm.FireCommand{
				Machine:        "order",
				MachineVersion: "v1",
				EntityID:       "order-race-1",
				Event:          "PAY_SUCCESS",
				Actor:          fsm.Actor{ID: fmt.Sprintf("u-%d", i), Role: "customer"},
				IdempotencyKey: fmt.Sprintf("race-%d", i),
				Payload:        map[string]any{"paymentStatus": "SUCCESS", "amount": 100},
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var conflict fsm.ErrConcurrentTransition
		if errors.As(err, &conflict) || errors.As(err, &fsm.ErrInvalidTransition{}) {
			conflicts++
			continue
		}
		t.Fatalf("unexpected error: %v", err)
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one success and one conflict, got successes=%d conflicts=%d", successes, conflicts)
	}
	assertCount(t, db, "fsm_state_log", 1)
	assertCount(t, db, "fsm_outbox", 1)
}

func startMySQL(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.4",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "root",
			"MYSQL_DATABASE":      "fsm_test",
			"MYSQL_USER":          "fsm",
			"MYSQL_PASSWORD":      "fsm",
		},
		WaitingFor: wait.ForListeningPort("3306/tcp").WithStartupTimeout(2 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate mysql container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("fsm:fsm@tcp(%s:%s)/fsm_test?parseTime=true", host, port.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("close db: %v", err)
		}
	})

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.PingContext(ctx); err == nil {
			return db
		}
		time.Sleep(time.Second)
	}
	t.Fatal("mysql did not become ready")
	return nil
}

func newRuntime(t *testing.T, repo fsm.Repository) *fsm.Runtime {
	t.Helper()
	registry := fsm.NewActionRegistry()
	actions.RegisterOutbox(registry, map[string]string{
		"outbox.order_paid": "order.paid",
	})
	spec, err := fsm.LoadYAML("../../configs/order.v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	machine, err := fsm.Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	runtime := fsm.NewRuntime(repo, registry)
	runtime.RegisterMachine(machine)
	return runtime
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("expected %s count %d, got %d", table, want, got)
	}
}
