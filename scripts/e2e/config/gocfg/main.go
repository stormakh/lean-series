// Prints Go's view of config files, for comparison with the Rust port (crates/config/examples/cfgdump.rs).
// Modes: "load <files>" (LoadFile + String(), or ERR: <error>), "agent <files>" (agent mode),
// "scrape <files>" (GetScrapeConfigs as YAML).
package main

import (
	"fmt"
	"os"

	"github.com/prometheus/common/promslog"
	"go.yaml.in/yaml/v2"

	"github.com/prometheus/prometheus/config"
	_ "github.com/prometheus/prometheus/discovery/install"
)

func main() {
	mode := os.Args[1]
	for _, f := range os.Args[2:] {
		fmt.Printf("=== %s\n", f)
		c, err := config.LoadFile(f, mode == "agent", promslog.NewNopLogger())
		if err != nil {
			fmt.Printf("ERR: %s\n", err)
			continue
		}
		switch mode {
		case "load", "agent":
			fmt.Print(c.String())
		case "scrape":
			scs, err := c.GetScrapeConfigs()
			if err != nil {
				fmt.Printf("ERR: %s\n", err)
				continue
			}
			b, _ := yaml.Marshal(scs)
			fmt.Print(string(b))
		}
	}
}
