package workflow

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Machine 是编译后的流程模型。运行时只读取 Machine，不再解释原始 YAML。
type Machine struct {
	Name        string
	Version     string
	Initial     string
	States      map[string]StateDefinition
	Events      map[string]EventDefinition
	Tasks       map[string]TaskDefinition
	Transitions map[string]Transition
}

// LoadYAML 读取 workflow-go 的 YAML 定义。
func LoadYAML(raw []byte) (*WorkflowDefinition, error) {
	var def WorkflowDefinition
	if err := yaml.Unmarshal(raw, &def); err != nil {
		return nil, err
	}
	return &def, nil
}

// Compile 校验并编译流程定义。这里会把名称索引好，避免运行时反复扫描。
func Compile(def *WorkflowDefinition) (*Machine, error) {
	if def == nil {
		return nil, ErrInvalidDefinition{Message: "definition is nil"}
	}
	if def.Name == "" || def.Version == "" || def.Initial == "" {
		return nil, ErrInvalidDefinition{Message: "name, version and initial are required"}
	}
	m := &Machine{
		Name:        def.Name,
		Version:     def.Version,
		Initial:     def.Initial,
		States:      map[string]StateDefinition{},
		Events:      map[string]EventDefinition{},
		Tasks:       map[string]TaskDefinition{},
		Transitions: map[string]Transition{},
	}
	for _, state := range def.States {
		if state.Name == "" {
			return nil, ErrInvalidDefinition{Message: "state name is required"}
		}
		if _, exists := m.States[state.Name]; exists {
			return nil, ErrInvalidDefinition{Message: "duplicate state: " + state.Name}
		}
		m.States[state.Name] = state
	}
	if _, ok := m.States[def.Initial]; !ok {
		return nil, ErrInvalidDefinition{Message: "initial state not found: " + def.Initial}
	}
	for _, event := range def.Events {
		if event.Name == "" {
			return nil, ErrInvalidDefinition{Message: "event name is required"}
		}
		if _, exists := m.Events[event.Name]; exists {
			return nil, ErrInvalidDefinition{Message: "duplicate event: " + event.Name}
		}
		m.Events[event.Name] = event
	}
	for _, task := range def.Tasks {
		if task.Name == "" || task.Handler == "" {
			return nil, ErrInvalidDefinition{Message: "task name and handler are required"}
		}
		if task.Retry.MaxAttempts <= 0 {
			task.Retry.MaxAttempts = 1
		}
		if task.Retry.Backoff <= 0 {
			task.Retry.Backoff = time.Second
		}
		if _, exists := m.Tasks[task.Name]; exists {
			return nil, ErrInvalidDefinition{Message: "duplicate task: " + task.Name}
		}
		m.Tasks[task.Name] = task
	}
	for _, state := range m.States {
		for _, taskName := range state.OnEnter {
			if _, ok := m.Tasks[taskName]; !ok {
				return nil, ErrInvalidDefinition{Message: fmt.Sprintf("state %s references unknown task %s", state.Name, taskName)}
			}
		}
	}
	for _, task := range m.Tasks {
		if task.Compensation != "" {
			if _, ok := m.Tasks[task.Compensation]; !ok {
				return nil, ErrInvalidDefinition{Message: fmt.Sprintf("task %s references unknown compensation %s", task.Name, task.Compensation)}
			}
		}
		if task.OnSuccess != "" {
			if _, ok := m.Events[task.OnSuccess]; !ok {
				return nil, ErrInvalidDefinition{Message: fmt.Sprintf("task %s references unknown success event %s", task.Name, task.OnSuccess)}
			}
		}
		if task.OnFailure != "" {
			if _, ok := m.Events[task.OnFailure]; !ok {
				return nil, ErrInvalidDefinition{Message: fmt.Sprintf("task %s references unknown failure event %s", task.Name, task.OnFailure)}
			}
		}
	}
	for _, transition := range def.Transitions {
		if transition.Name == "" || transition.From == "" || transition.Event == "" || transition.To == "" {
			return nil, ErrInvalidDefinition{Message: "transition name, from, event and to are required"}
		}
		if _, ok := m.States[transition.From]; !ok {
			return nil, ErrInvalidDefinition{Message: "transition from state not found: " + transition.From}
		}
		if _, ok := m.States[transition.To]; !ok {
			return nil, ErrInvalidDefinition{Message: "transition to state not found: " + transition.To}
		}
		if _, ok := m.Events[transition.Event]; !ok {
			return nil, ErrInvalidDefinition{Message: "transition event not found: " + transition.Event}
		}
		key := transitionKey(transition.From, transition.Event)
		if _, exists := m.Transitions[key]; exists {
			return nil, ErrInvalidDefinition{Message: "duplicate transition for " + key}
		}
		m.Transitions[key] = transition
	}
	return m, nil
}

func transitionKey(state string, event string) string {
	return state + ":" + event
}
