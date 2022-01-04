package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

func main() {

	// add the Amazon Keyspaces service endpoint
	cluster := gocql.NewCluster("cassandra.eu-west-1.amazonaws.com:9142")
	// add your service specific credentials
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cdk-at-651604682962",
		Password: "WnMqn7uW0Q7iJZtWio202rJpRslH3yNnmUk8TYx5YFo="}
	// provide the path to the sf-class2-root.crt
	cluster.SslOpts = &gocql.SslOptions{
		CaPath: "sf-class2-root.crt",
	}
	// Override default Consistency to LocalQuorum
	cluster.Consistency = gocql.LocalQuorum
	// Disable initial host lookup
	cluster.DisableInitialHostLookup = true
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("err>", err)
	}
	defer session.Close()
	//add data to table
	t1 := time.Now()
	iter := session.Query("INSERT INTO bitsouks_test_table.user_table (id, email, age) VALUES (1, 'john@example.com', 21);").Iter()
	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Println("TIME TO Add", diff)
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	t1 = time.Now()
	iter = session.Query("SELECT * FROM bitsouks_test_table.user_table;").Iter()
	t2 = time.Now()
	diff = t2.Sub(t1)
	fmt.Println("TIME TO FETCH", diff)
	var id string
	var email string
	var age string
	for iter.Scan(&id, &email, &age) {
		fmt.Println("user fetched:", id, email, age)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	session.Close()
}
