package workflow

import (
	"fmt"
	"strings"
)

// Mermaid 输出流程定义图，用于 README、文档和排查页面展示。
func Mermaid(machine *Machine) string {
	var b strings.Builder
	b.WriteString("stateDiagram-v2\n")
	fmt.Fprintf(&b, "    [*] --> %s\n", machine.Initial)
	for _, state := range machine.States {
		for _, task := range state.OnEnter {
			taskNode := state.Name + "_" + task
			fmt.Fprintf(&b, "    %s --> %s: on enter\n", state.Name, taskNode)
			fmt.Fprintf(&b, "    %s --> %s: task\n", taskNode, state.Name)
		}
	}
	for _, transition := range machine.Transitions {
		fmt.Fprintf(&b, "    %s --> %s: %s\n", transition.From, transition.To, transition.Event)
	}
	return b.String()
}
