package engine

import (
	"context"
	"time"
)

type PersistenceDB interface {
	PublishMessage(ctx context.Context, topic TopicID, msg Message, receivers []ActorID) error
	GetUnfinishedHandlings(ctx context.Context, receiverID ActorID) ([]MessageHandling, error)
	GetMessage(ctx context.Context, topic TopicID, msgID MessageID) (Message, error)
	UpdateHandlingStatus(ctx context.Context, topic TopicID, msgID MessageID, receiverID ActorID, status MessageHandlingStatus, retries int) error
	GetMessagesSince(ctx context.Context, topic TopicID, since time.Time) ([]Message, error)
	DeleteExpiredMessages(ctx context.Context) error
}
