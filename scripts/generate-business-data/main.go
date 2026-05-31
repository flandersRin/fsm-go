package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type workflowSeed struct {
	InstanceID string         `json:"instance_id"`
	Workflow   string         `json:"workflow"`
	Version    string         `json:"version"`
	Data       map[string]any `json:"data"`
}

func main() {
	kind := flag.String("kind", "order", "数据类型：order、async_task、saga、agent、failure")
	count := flag.Int("count", 1000, "生成数量")
	out := flag.String("out", "business-data.jsonl", "输出文件")
	flag.Parse()

	file, err := os.Create(*out)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for i := 0; i < *count; i++ {
		item := seed(*kind, i)
		if err := enc.Encode(item); err != nil {
			panic(err)
		}
	}
	fmt.Printf("generated=%d kind=%s out=%s\n", *count, *kind, *out)
}

func seed(kind string, i int) workflowSeed {
	switch kind {
	case "async_task":
		return workflowSeed{InstanceID: fmt.Sprintf("task-%06d", i), Workflow: "async_report", Version: "v1", Data: map[string]any{"report_id": i}}
	case "saga":
		return workflowSeed{InstanceID: fmt.Sprintf("saga-%06d", i), Workflow: "saga_order", Version: "v1", Data: map[string]any{"order_id": i, "amount": 100 + i%50}}
	case "agent":
		return workflowSeed{InstanceID: fmt.Sprintf("agent-%06d", i), Workflow: "agent_run", Version: "v1", Data: map[string]any{"prompt_id": i}}
	case "failure":
		return workflowSeed{InstanceID: fmt.Sprintf("failure-%06d", i), Workflow: "failure_case", Version: "v1", Data: map[string]any{"should_fail": true, "attempt": 0}}
	default:
		return workflowSeed{InstanceID: fmt.Sprintf("order-%06d", i), Workflow: "order", Version: "v1", Data: map[string]any{"order_id": i, "amount": 100 + i%100}}
	}
}
