package engine

import (
	"fmt"
)

type ActorID string

type Producer interface {
	Start(engineCtx EngineCtx) error
}

type Receiver[T TopicProvider] interface {
	Receive(ctx EngineCtx, msg T) error
}

type rawReceiver interface {
	receive(ctx EngineCtx, msg Message) error
}

type typedReceiver[T TopicProvider] struct {
	impl Receiver[T]
}

func (r *typedReceiver[T]) receive(ctx EngineCtx, msg Message) error {
	payload, ok := msg.Payload().(T)
	if !ok {
		return fmt.Errorf("unexpected payload type %T", msg.Payload())
	}
	return r.impl.Receive(ctx, payload)
}

type Actor struct {
	id       ActorID
	producer Producer
	receiver rawReceiver

	listenedTopic TopicID
}

func (a *Actor) ID() ActorID {
	return a.id
}

func (a *Actor) GetProducer() Producer {
	return a.producer
}

func (a *Actor) GetReceiver() rawReceiver {
	return a.receiver
}

func (a *Actor) HasProducer() bool {
	return a.producer != nil
}

func (a *Actor) HasReceiver() bool {
	return a.receiver != nil
}

func (a *Actor) ListenedTopic() TopicID {
	return a.listenedTopic
}

type ProducerReceiver[T TopicProvider] interface {
	Producer
	Receiver[T]
}

// topicOf derives the topic a Receiver[T] listens to from T itself, so the
// subscription can never disagree with the message type. This relies on the
// TopicProvider contract: Topic() must return a constant and be safe to call on
// the zero value of T (typically a nil pointer).
func topicOf[T TopicProvider]() TopicID {
	var zero T
	return zero.Topic()
}

func CreateProducerReceiverActor[T TopicProvider](id ActorID, impl ProducerReceiver[T]) *Actor {
	return &Actor{
		id:            id,
		producer:      impl,
		receiver:      &typedReceiver[T]{impl: impl},
		listenedTopic: topicOf[T](),
	}
}

func CreateProducerActor(id ActorID, impl Producer) *Actor {
	return &Actor{
		id:       id,
		producer: impl,
		receiver: nil,
	}
}

func CreateReceiverActor[T TopicProvider](id ActorID, impl Receiver[T]) *Actor {
	return &Actor{
		id:            id,
		producer:      nil,
		receiver:      &typedReceiver[T]{impl: impl},
		listenedTopic: topicOf[T](),
	}
}
