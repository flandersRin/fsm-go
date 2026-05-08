package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/flandersrin/fsm-go/actions"
	"github.com/flandersrin/fsm-go/fsm"
	mysqlrepo "github.com/flandersrin/fsm-go/persistence/mysql"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("FSM_MYSQL_DSN")
	if dsn == "" {
		dsn = "fsm:fsm@tcp(127.0.0.1:3306)/fsm_demo?parseTime=true"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("close db: %v", err)
		}
	}()

	repo := mysqlrepo.NewRepository(db)
	if err := waitForDB(ctx, db); err != nil {
		log.Fatal(err)
	}
	if err := repo.InitSchema(ctx); err != nil {
		log.Fatal(err)
	}

	registry := fsm.NewActionRegistry()
	actions.RegisterOutbox(registry, map[string]string{
		"outbox.order_paid":      "order.paid",
		"outbox.order_shipped":   "order.shipped",
		"outbox.order_completed": "order.completed",
		"outbox.order_cancelled": "order.cancelled",
	})

	runtime := fsm.NewRuntime(repo, registry)
	spec, err := fsm.LoadYAML("configs/order.v1.yaml")
	if err != nil {
		log.Fatal(err)
	}
	machine, err := fsm.Compile(spec)
	if err != nil {
		log.Fatal(err)
	}
	runtime.RegisterMachine(machine)

	server := &demoServer{runtime: runtime, db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("POST /demo/order/init", server.initOrder)
	mux.HandleFunc("POST /demo/order/fire", server.fireOrder)
	mux.HandleFunc("GET /demo/order/", server.getOrder)
	mux.HandleFunc("GET /demo/outbox", server.listOutbox)

	log.Println("fsm demo listening on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

type demoServer struct {
	runtime *fsm.Runtime
	db      *sql.DB
}

type initOrderRequest struct {
	EntityID string         `json:"entity_id"`
	Data     map[string]any `json:"data"`
}

type fireOrderRequest struct {
	EntityID       string         `json:"entity_id"`
	Event          string         `json:"event"`
	ActorID        string         `json:"actor_id"`
	ActorRole      string         `json:"actor_role"`
	RequestID      string         `json:"request_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	Payload        map[string]any `json:"payload"`
}

func (s *demoServer) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *demoServer) initOrder(w http.ResponseWriter, r *http.Request) {
	var req initOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.EntityID == "" {
		writeErrorMessage(w, http.StatusBadRequest, "entity_id is required")
		return
	}
	if req.Data == nil {
		req.Data = map[string]any{}
	}
	err := s.runtime.CreateEntity(r.Context(), fsm.StateEntity{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       req.EntityID,
		State:          "PENDING",
		Revision:       0,
		Data:           req.Data,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"entity_id": req.EntityID, "state": "PENDING"})
}

func (s *demoServer) fireOrder(w http.ResponseWriter, r *http.Request) {
	var req fireOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := s.runtime.Fire(r.Context(), fsm.FireCommand{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       req.EntityID,
		Event:          req.Event,
		Actor:          fsm.Actor{ID: req.ActorID, Role: req.ActorRole},
		RequestID:      req.RequestID,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
	})
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *demoServer) getOrder(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/demo/order/")
	if strings.HasSuffix(path, "/logs") {
		s.listLogs(w, strings.TrimSuffix(path, "/logs"))
		return
	}

	row := s.db.QueryRowContext(r.Context(), `
SELECT machine, machine_version, entity_id, state, revision, COALESCE(data, JSON_OBJECT()), create_at, update_at
FROM fsm_entity
WHERE machine = 'order' AND entity_id = ? AND deleted = 0
`, path)
	var raw json.RawMessage
	entity := fsm.StateEntity{}
	if err := row.Scan(&entity.Machine, &entity.MachineVersion, &entity.EntityID, &entity.State, &entity.Revision, &raw, &entity.CreatedAt, &entity.UpdatedAt); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	_ = json.Unmarshal(raw, &entity.Data)
	writeJSON(w, http.StatusOK, entity)
}

func (s *demoServer) listLogs(w http.ResponseWriter, entityID string) {
	rows, err := s.db.Query(`
SELECT event, from_state, to_state, transition_name, actor_id, request_id, idempotency_key, create_at
FROM fsm_state_log
WHERE machine = 'order' AND entity_id = ? AND deleted = 0
ORDER BY id
`, entityID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("close log rows: %v", err)
		}
	}()

	var logs []map[string]any
	for rows.Next() {
		var event, fromState, toState, transitionName string
		var actorID, requestID, idem sql.NullString
		var createdAt time.Time
		if err := rows.Scan(&event, &fromState, &toState, &transitionName, &actorID, &requestID, &idem, &createdAt); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		logs = append(logs, map[string]any{
			"event":           event,
			"from_state":      fromState,
			"to_state":        toState,
			"transition_name": transitionName,
			"actor_id":        actorID.String,
			"request_id":      requestID.String,
			"idempotency_key": idem.String,
			"create_at":       createdAt,
		})
	}
	writeJSON(w, http.StatusOK, logs)
}

func (s *demoServer) listOutbox(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
SELECT id, topic, msg_key, payload, status, retry_count, create_at
FROM fsm_outbox
WHERE deleted = 0
ORDER BY id DESC
LIMIT 50
`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("close outbox rows: %v", err)
		}
	}()

	var out []map[string]any
	for rows.Next() {
		var id int64
		var topic, key, status string
		var payload json.RawMessage
		var retryCount int
		var createdAt time.Time
		if err := rows.Scan(&id, &topic, &key, &payload, &status, &retryCount, &createdAt); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		out = append(out, map[string]any{
			"id":          id,
			"topic":       topic,
			"key":         key,
			"payload":     payload,
			"status":      status,
			"retry_count": retryCount,
			"create_at":   createdAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func waitForDB(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeErrorMessage(w, status, err.Error())
}

func writeErrorMessage(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
