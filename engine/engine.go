package engine

import (
	"context"
	"log"
	"sync"
)

type EngineCtx struct {
	ctx context.Context
	bus *engineBus
}

func (e *EngineCtx) Context() context.Context {
	return e.ctx
}

func (e *EngineCtx) PublishMsg(ctx context.Context, msg Message) error {
	return e.bus.publishMsg(ctx, msg)
}

type Engine struct {
	engineCtx     EngineCtx
	executionCtx  context.Context
	bus           *engineBus
	persistenceDB PersistenceDB

	actors map[ActorID]*Actor
}

func NewEngine(persistenceDB PersistenceDB) *Engine {
	bus := &engineBus{
		topics: make(map[TopicID]*Topic),
	}

	return &Engine{
		persistenceDB: persistenceDB,
		actors:        make(map[ActorID]*Actor),
		bus:           bus,
	}
}

func (e *Engine) RegisterActor(actor *Actor) error {
	if actor.HasProducer() {
		e.actors[actor.ID()] = actor
	}
	if actor.HasReceiver() {
		e.actors[actor.ID()] = actor
	}

	return nil
}

func (e *Engine) Start(ctx context.Context) error {
	engineCtx := EngineCtx{
		ctx: ctx,
		bus: e.bus,
	}
	e.engineCtx = engineCtx

	wg := sync.WaitGroup{}
	for _, actor := range e.actors {
		if actor.HasReceiver() {
			e.bus.linkReceiverWithTopics(e.engineCtx, actor.ID(), actor.GetReceiver(), actor.GetListenedTopics()...)
		}

		if actor.HasProducer() {
			wg.Go(func() {
				if err := actor.GetProducer().Start(e.engineCtx); err != nil {
					log.Printf("failed to start producer: %v", err)
				}
			})
		}
	}
	wg.Go(func() {
		e.bus.start(ctx)
	})

	wg.Wait()

	return nil
}
