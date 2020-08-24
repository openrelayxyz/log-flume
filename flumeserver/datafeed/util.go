package datafeed

func OrderedProcessor(start uint64, buffer int, itemGetter func(uint64, chan<- interface{}, func()), orderedProcessor func(interface{})) {
  orderedResults := make(chan chan interface{}, buffer)
  semaphore := make(chan struct{}, buffer)
  quit := make(chan struct{})
  go func() {
    for i := start ; ; i++ {
      // Add quit chan
      select {
      case <-quit:
        close(orderedResults)
        break
      case semaphore <- struct{}{}:
      }
      ch := make(chan interface{})
      orderedResults <- ch
      go itemGetter(i, ch, func() { quit <- struct{}{} })
    }
  }()
  for ch := range orderedResults {
    <-semaphore
    orderedProcessor(<-ch)
  }
}
