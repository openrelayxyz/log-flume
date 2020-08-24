package datafeed

func Counter(buffer int) (<-chan uint64, func()) {
  ch := make(chan uint64, buffer)
  quit := make(chan struct{})
  go func() {
    for i := 0; ; i++ {
      select {
      case <-quit:
        close(ch)
        return
      case ch <- i:
      }
    }
  }
  return ch, func() { quit <- struct{}{} }
}
