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
  tokenListsPath := path.Join("/tmp", "tokenlist", "tokens")
  fmt.Println(tokenListsPath)
  out, _ := os.Create("tokens.go")
  out.Write([]byte("package tokens\n\nfunc init() {\n"))
  genNetwork(out, 1, path.Join(tokenListsPath, "eth"))
  genNetwork(out, 61, path.Join(tokenListsPath, "etc"))
  genNetwork(out, 3, path.Join(tokenListsPath, "rop"))
  genNetwork(out, 4, path.Join(tokenListsPath, "rin"))
  genNetwork(out, 5, path.Join(tokenListsPath, "gor"))
  out.Write([]byte("}"))
}
