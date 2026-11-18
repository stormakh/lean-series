package main

import (
	"fmt"
	"os"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"go.yaml.in/yaml/v2"
)

type durs struct {
	A model.Duration `yaml:"a,omitempty"`
}

func main() {
	mode := os.Args[1]
	for _, f := range os.Args[2:] {
		b, err := os.ReadFile(f)
		if err != nil {
			panic(err)
		}
		fmt.Printf("=== %s\n", f)
		switch mode {
		case "http":
			c, err := config.LoadHTTPConfig(string(b))
			if err != nil {
				fmt.Printf("ERR: %s\n", err)
				continue
			}
			fmt.Print(c.String())
		case "any", "anystrict":
			var v interface{}
			if mode == "any" {
				err = yaml.Unmarshal(b, &v)
			} else {
				err = yaml.UnmarshalStrict(b, &v)
			}
			if err != nil {
				fmt.Printf("ERR: %s\n", err)
				continue
			}
			out, err := yaml.Marshal(v)
			if err != nil {
				fmt.Printf("MERR: %s\n", err)
				continue
			}
			fmt.Print(string(out))
		case "str":
			// each line of the file is a string value; marshal it as a map value and a list item
			out, _ := yaml.Marshal(map[string]interface{}{"k": string(b), string(b): 1, "l": []string{string(b)}})
			fmt.Print(string(out))
		}
	}
}
