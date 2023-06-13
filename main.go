package main

import (
	"fmt"
	"log"

	"motley/motleyql"
)

func main() {
	query, err := motleyql.Parse("SELECT * FROM models WHERE e = '1' AND f > '2'")
	if err != nil {
		fmt.Printf("wang")
		log.Fatal(err)
	}
	fmt.Printf("%+#v", query)
}
