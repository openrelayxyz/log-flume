package datafeed

import (
  "os"
  "os/signal"
  "syscall"
  "log"
)

func OrderedProcessor(start uint64, buffer int, itemGetter func(uint64, chan<- interface{}, func()), orderedProcessor func(interface{})) {
  orderedResults := make(chan chan interface{}, buffer)
  semaphore := make(chan struct{}, buffer)
  quit := make(chan struct{})
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
  go func() {
    for i := start ; ; i++ {
      // Add quit chan
      select {
      case <-sigs:
        log.Printf("Received Interrupt, stopping...")
        semaphore <- struct{}{}
        return
      case <-quit:
        close(orderedResults)
        semaphore <- struct{}{}
        return
      case semaphore <- struct{}{}:
      }
      ch := make(chan interface{})
      orderedResults <- ch
      quitter := func() {
        select {
        case quit <- struct{}{}:
        default:
        }
        ch <- nil
      }
      go itemGetter(i, ch, quitter)
    }
  }()
  for ch := range orderedResults {
    <-semaphore
    item := <-ch
    if item != nil {
      orderedProcessor(item)
    }
  }
}
