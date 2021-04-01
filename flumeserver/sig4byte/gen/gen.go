package main
import (
  "fmt"
  "os"
  "path"
  "io"
  "io/ioutil"
)

func genNetwork(out *os.File, net int, netpath string) {
  fs, _ := ioutil.ReadDir(netpath)
  for _, f := range fs {
    out.Write([]byte(fmt.Sprintf("register(%v, []byte(`", net)))
    f, _ := os.Open(path.Join(netpath, f.Name()))
    io.Copy(out, f)
    out.Write([]byte("`))\n"))
  }
}

func main() {
  sigPaths := path.Join("/tmp", "4bytes", "signatures")
  out, _ := os.Create("sigs.go")
  out.Write([]byte("package sig4byte\n\nfunc init() {\n"))
  fs, _ := ioutil.ReadDir(sigPaths)
  for _, f := range fs {
    sig := f.Name()
    f, _ := os.Open(path.Join(sigPaths, f.Name()))
    data, _ := ioutil.ReadAll(f)
    f.Close()
    if len(data) == 0 { continue }
    out.Write([]byte(fmt.Sprintf("register(\"%v\", []byte(`", sig)))
    out.Write(data)
    out.Write([]byte("`))\n"))
  }
  out.Write([]byte("}"))
}
