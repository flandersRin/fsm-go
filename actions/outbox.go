package actions

import (
	"context"
	"fmt"

	"github.com/flandersrin/fsm-go/fsm"
)

func RegisterOutbox(registry *fsm.ActionRegistry, topicByName map[string]string) {
	for name, topic := range topicByName {
		actionName := name
		actionTopic := topic
		registry.Register(actionName, func(ctx context.Context, ac fsm.ActionContext) error {
			return ac.Tx.InsertOutbox(ctx, fsm.OutboxMessage{
				Topic: actionTopic,
				Key:   fmt.Sprintf("%s:%s", ac.Command.Machine, ac.Command.EntityID),
				Payload: map[string]any{
					"machine":         ac.Command.Machine,
					"machine_version": ac.Command.MachineVersion,
					"entity_id":       ac.Command.EntityID,
					"event":           ac.Command.Event,
					"from_state":      ac.Result.FromState,
					"to_state":        ac.Result.ToState,
					"transition_name": ac.Result.TransitionName,
					"request_id":      ac.Command.RequestID,
				},
			})
		})
	}
}
