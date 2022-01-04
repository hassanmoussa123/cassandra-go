package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

func main() {

	// add the Amazon Keyspaces service endpoint
	cluster := gocql.NewCluster("18.202.32.58:9042")
	// add your service specific credentials
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cdk-at-651604682962",
		Password: "WnMqn7uW0Q7iJZtWio202rJpRslH3yNnmUk8TYx5YFo="}

	// Override default Consistency to LocalQuorum
	cluster.Consistency = gocql.LocalQuorum
	// Disable initial host lookup
	cluster.DisableInitialHostLookup = true
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("err>", err)
	}

	defer session.Close()
	iter := session.Query("CREATE KEYSPACE IF NOT EXISTS bitsouks_test_table WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};").Iter()
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	iter = session.Query(" CREATE TABLE IF NOT EXISTS bitsouks_test_table.user_table (id int,email text,age int,PRIMARY KEY (id));").Iter()
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	//add data to table
	t1 := time.Now()
	iter = session.Query("INSERT INTO bitsouks_test_table.user_table (id, email, age) VALUES (1, 'john@example.com', 21);").Iter()
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
