package engine

import (
	"context"
)

type PersistenceDB interface {
	PublishMessage(ctx context.Context, topic TopicID, msg Message, receivers []ActorID) error
	GetHandling(ctx context.Context, topic TopicID, msgID MessageID, receiverID ActorID) (*MessageHandling, error)
	GetUnfinishedHandlings(ctx context.Context, receiverID ActorID) ([]MessageHandling, error)
	GetMessage(ctx context.Context, topic TopicID, msgID MessageID) (Message, error)
	UpdateHandlingStatus(ctx context.Context, topic TopicID, msgID MessageID, receiverID ActorID, status MessageHandlingStatus, retries int) error
	DeleteExpiredMessages(ctx context.Context) error
}
