package main

import (
	"encoding/json"
	"os"
	"strconv"
)

func main() {
	f, err := os.Open("/Users/n8maninger/Downloads/hostd.log")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	for {
		var v map[string]any
		if err := dec.Decode(&v); err != nil {
			break
		}

		ts, _ := v["ts"].(string)
		f, err := strconv.ParseFloat(ts, 64)
		if err != nil {
			continue
		} else if f < 1693360589.0406103 {
			continue
		}
		enc.Encode(v)
	}
}
