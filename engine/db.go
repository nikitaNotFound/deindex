package engine

import (
	"context"
	"time"
)

type UpdateHandlingParams struct {
	TopicID    TopicID
	MessageID  MessageID
	ReceiverID ActorID
	Status     MessageHandlingStatus
	Retries    int
	ExecuteAt  *time.Time
}

type EngineDB interface {
	WrapTx(ctx context.Context, fn func(ctx context.Context) error) error
	CreateMessage(ctx context.Context, topic TopicID, msg Message, receivers []ActorID) error
	CreateHandling(ctx context.Context, topic TopicID, msgID MessageID, receiverID ActorID) error
	GetHandling(ctx context.Context, topic TopicID, msgID MessageID, receiverID ActorID) (*MessageHandling, error)
	GetUnfinishedHandlings(ctx context.Context, receiverID ActorID) ([]MessageHandling, error)
	GetMessage(ctx context.Context, topic TopicID, msgID MessageID) (Message, error)
	UpdateHandlingStatus(ctx context.Context, params UpdateHandlingParams) error
	DeleteExpiredMessages(ctx context.Context) error
}
