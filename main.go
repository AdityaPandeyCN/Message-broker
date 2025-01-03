package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Event struct {
   Topic string
   Data  interface{}
}

type EventBus struct {
   topics map[string][]chan Event
   mu     sync.RWMutex
   ctx    context.Context
   cancel context.CancelFunc
}

func NewEventBus() *EventBus {
   ctx, cancel := context.WithCancel(context.Background())
   return &EventBus{
       topics:  make(map[string][]chan Event),
       ctx:     ctx,
       cancel:  cancel,
   }
}

func (eb *EventBus) Subscribe(topic string, bufSize int) (<-chan Event, func()) {
   eb.mu.Lock()
   defer eb.mu.Unlock()

   ch := make(chan Event, bufSize)
   eb.topics[topic] = append(eb.topics[topic], ch)

   unsubscribe := func() {
       eb.mu.Lock()
       defer eb.mu.Unlock()

       for i, sub := range eb.topics[topic] {
           if sub == ch {
               close(ch)
               eb.topics[topic] = append(eb.topics[topic][:i], eb.topics[topic][i+1:]...)
               break
           }
       }
       fmt.Printf("Unsubscribed from topic: %s\n", topic)
   }

   return ch, unsubscribe
}

func (eb *EventBus) Publish(event Event) {
   eb.mu.RLock()
   defer eb.mu.RUnlock()

   for _, ch := range eb.topics[event.Topic] {
       select {
       case ch <- event:
           fmt.Printf("Published message to topic %s: %v\n", event.Topic, event.Data)
       default:
           fmt.Printf("Dropped message for topic %s due to full buffer\n", event.Topic)
       }
   }
}

func main() {
   bus := NewEventBus()

   ch1, unsub1 := bus.Subscribe("orders", 10)
   ch2, unsub2 := bus.Subscribe("orders", 10) 

   // First subscriber
   go func() {
       for event := range ch1 {
           fmt.Printf("Subscriber 1 received: %v\n", event.Data)
       }
   }()

   // Second subscriber
   go func() {
       for event := range ch2 {
           fmt.Printf("Subscriber 2 received: %v\n", event.Data)
       }
   }()

   // Publishing test messages
   bus.Publish(Event{Topic: "orders", Data: "Hello World!"})
   bus.Publish(Event{Topic: "orders", Data: "This is a test message"})
   bus.Publish(Event{Topic: "orders", Data: "Message broker working!"})
   
   // Wait for messages to be processed
   time.Sleep(2 * time.Second)
   
   // Unsubscribe both subscribers
   unsub1()
   unsub2()

   // Try publishing after unsubscribe
   bus.Publish(Event{Topic: "orders", Data: "This won't be received"})
   
   time.Sleep(time.Second)
}