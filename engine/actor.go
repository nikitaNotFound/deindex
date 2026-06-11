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

	listenedTopics []TopicID
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

func (a *Actor) GetListenedTopics() []TopicID {
	return a.listenedTopics
}

type CreateReceiverActorParams struct {
	listenedTopics []TopicID
}
type CreateReceiverActorOpt func(*CreateReceiverActorParams)

func WithListenedTopics(topics ...TopicID) CreateReceiverActorOpt {
	return func(params *CreateReceiverActorParams) {
		params.listenedTopics = topics
	}
}

type ProducerReceiver[T TopicProvider] interface {
	Producer
	Receiver[T]
}

func CreateProducerReceiverActor[T TopicProvider](id ActorID, impl ProducerReceiver[T], opts ...CreateReceiverActorOpt) *Actor {
	params := handleCreateReceiverParams(opts...)

	return &Actor{
		id:             id,
		producer:       impl,
		receiver:       &typedReceiver[T]{impl: impl},
		listenedTopics: params.listenedTopics,
	}
}

func CreateProducerActor(id ActorID, impl Producer) *Actor {
	return &Actor{
		id:       id,
		producer: impl,
		receiver: nil,
	}
}

func CreateReceiverActor[T TopicProvider](id ActorID, impl Receiver[T], opts ...CreateReceiverActorOpt) *Actor {
	params := handleCreateReceiverParams(opts...)

	return &Actor{
		id:             id,
		producer:       nil,
		receiver:       &typedReceiver[T]{impl: impl},
		listenedTopics: params.listenedTopics,
	}
}

func handleCreateReceiverParams(opts ...CreateReceiverActorOpt) *CreateReceiverActorParams {
	params := &CreateReceiverActorParams{
		listenedTopics: []TopicID{},
	}
	for _, opt := range opts {
		opt(params)
	}

	return params
}
