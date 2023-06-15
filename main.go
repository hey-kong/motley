package main

import (
	"fmt"
	"log"

	"github.com/hey-kong/motley/motleyql"
)

func main() {
	query1, err := motleyql.Parse("SELECT * FROM models")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+#v\n", query1)

	query2, err := motleyql.Parse("SELECT * FROM models WHERE task = object_detection AND data_type = image")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+#v\n", query2)

	query3, err := motleyql.Parse("SELECT * FROM models WHERE task = object_detection AND data_type = image " +
		"ORDER BY n_param DESC LIMIT 1 USING local_data RESPOND IN fast_mode")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+#v\n", query3)
}
