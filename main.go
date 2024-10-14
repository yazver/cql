// Выполнение запросов в Scylla.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

const usage = `Usage: cql [options] QUERY
	If username or password flags is not provided, then the authentication is not used.

	-h --host 		Specifies the host name and port of the machine on which the server is running. Default value is 127.0.0.1:9042.
	-u --username 	Authenticate as user.
	-p --password 	Authenticate using password.
	-k --keyspace 	Connect to defined keyspace. May be empty.
	-v --verbose 	Prints additional information about command running.

	--help 		Show this message.

	Example:
		cqlsh -h scylla -p 9042 -e "CREATE KEYSPACE some_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
	`

func main() {
	var verbose bool
	const verboseUsage = "Prints additional information about command running."
	flag.BoolVar(&verbose, "v", false, verboseUsage)
	flag.BoolVar(&verbose, "verbose", false, verboseUsage)

	var username string
	const userUsage = "Authenticate as user. May be empty."
	flag.StringVar(&username, "u", "", userUsage)
	flag.StringVar(&username, "username", "", userUsage)

	var password string
	const passwordUsage = "Authenticate using password. May be empty."
	flag.StringVar(&password, "p", "", passwordUsage)
	flag.StringVar(&password, "password", "", passwordUsage)

	var host string
	const hostUsage = "Specifies the host name of the machine on which the server is running. Default value is 127.0.0.1."
	flag.StringVar(&host, "h", "127.0.0.1", hostUsage)
	flag.StringVar(&host, "host", "127.0.0.1", hostUsage)

	var keyspace string
	const keyspaceUsage = "Connect to defined keyspace. May be empty."
	flag.StringVar(&keyspace, "k", "", keyspaceUsage)
	flag.StringVar(&keyspace, "keyspace", "", keyspaceUsage)

	flag.Usage = func() { fmt.Print(usage) }

	flag.Parse()

	query := flag.Arg(0)
	if query == "" {
		_, _ = fmt.Fprintln(os.Stderr, "Query is empty.")
		os.Exit(1)
	}

	if verbose {
		fmt.Printf("Connecting to '%s' and keyspace '%s' as user:'%s'\n", host, keyspace, username)
	}
	conn, err := getSession(host, keyspace, username, password)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Connection to '%s' failed: %s.\n", host, err)
		os.Exit(1)
	}
	defer conn.Close()
	if verbose {
		fmt.Printf("Connected to '%s'.\n", host)
	}

	if verbose {
		fmt.Printf("Executing query \"%s\"\n", query)
	}
	if err = conn.ExecStmt(query); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Query failed: %s.\n", err)
		os.Exit(1)
	}
	if verbose {
		fmt.Println("Query successful executed.")
	}
}

func getSession(host, keyspace string, username, password string) (gocqlx.Session, error) {
	cluster := gocql.NewCluster(host)
	if username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	if keyspace != "" {
		cluster.Keyspace = keyspace
	}

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return gocqlx.Session{}, fmt.Errorf("creating session: %w", err)
	}
	return session, nil
}
