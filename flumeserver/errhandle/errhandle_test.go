package errhandle

import (
  "testing"
  "fmt"
)

func giveError(b bool) (int, error) {
  if b {
    return 0, fmt.Errorf("given error")
  }
  return 1, nil
}
func BenchmarkCondition50p(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (error) {
      v, err := giveError(i % 2 == 0)
      if err != nil {
        return err
      }
      if v != 1 {
        b.Errorf("Unexpected v")
      }
      return nil
    }()
  }
}
func BenchmarkHandlerNoErr(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (err error) {
      defer HandleErr(&err)
      v := CheckAndAssign(giveError(false))
      if v != 1 {
        b.Errorf("Unexpected v")
      }
      return
    }()
  }
}
func BenchmarkConditionNoErr(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (error) {
      v, err := giveError(false)
      if err != nil {
        return err
      }
      if v != 1 {
        b.Errorf("Unexpected v")
      }
      return nil
    }()
  }
}
func BenchmarkHandlerAllErr(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (err error) {
      defer HandleErr(&err)
      v := CheckAndAssign(giveError(true))
      if v != 1 {
        b.Errorf("Unexpected v")
      }
      return
    }()
  }
}
func BenchmarkConditionAllErr(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (error) {
      v, err := giveError(true)
      if err != nil {
        return err
      }
      if v != 1 {
        b.Errorf("Unexpected v")
      }
      return nil
    }()
  }
}
func BenchmarkHandlerNoErr50x(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (err error) {
      defer HandleErr(&err)
      for j := 0; j < 50; j++ {
        v := CheckAndAssign(giveError(false))
        if v != 1 {
          b.Errorf("Unexpected v")
        }
      }
      return
    }()
  }
}
func BenchmarkConditionNoErr50x(b *testing.B){
  for i := 0; i < b.N; i++ {
    func() (error) {
      for j := 0; j < 50; j++ {
        v, err := giveError(false)
        if err != nil {
          return err
        }
        if v != 1 {
          b.Errorf("Unexpected v")
        }
      }
      return nil
    }()
  }
}
