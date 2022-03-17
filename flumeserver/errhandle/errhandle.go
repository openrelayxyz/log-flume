package errhandle

func Check(err error) {
    if err != nil {
        panic(err)
    }
}

func CheckAndAssign[T any](val T, err error) T {
  if err != nil {
      panic(err)
  }
  return val
}

func HandleErr(err *error) {
    e := recover()
    if  e == nil {
        return
    }
    if e, ok := e.(error); ok {
        *err = e
        return
    }
    panic(e)
}

func HandleErrFunc(fn func(err error)) {
  err := recover()
  if err != nil {
    if e, ok := err.(error); ok {
      fn(e)
      return
    }
    panic(err)
  }
}
