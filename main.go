// Выполнение запросов в Scylla.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

const usage = `Usage: cqlsh [options] [host [port]]
	If username or password flags is not provided, then the authentication is not used.
	The default host is localhost. The default port is 9042.

	-u --username 	Authenticate as user.
	-p --password 	Authenticate using password.
	-e --execute 	Execute the statement and quit.
	-k --keyspace 	Use keyspace.
	-h --help 		Show this message.

	Example:
		cqlsh scylla 9042 -e "CREATE KEYSPACE some_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
	`

func main() {
	const userUsage = "Authenticate as user."
	var username string
	flag.StringVar(&username, "u", "", userUsage)
	flag.StringVar(&username, "username", "", userUsage)

	const passwordUsage = "Authenticate using password."
	var password string
	flag.StringVar(&password, "p", "", passwordUsage)
	flag.StringVar(&password, "password", "", passwordUsage)

	var query string
	const executeUsage = "Execute the statement and quit."
	flag.StringVar(&query, "e", "", executeUsage)
	flag.StringVar(&query, "execute", "", executeUsage)

	var keyspace string
	const keyspaceUsage = "Connect to defined keyspace."
	flag.StringVar(&keyspace, "k", "", keyspaceUsage)
	flag.StringVar(&keyspace, "keyspace", "", keyspaceUsage)

	flag.Usage = func() { fmt.Print(usage) }

	flag.Parse()

	if query == "" {
		_, _ = fmt.Fprint(os.Stderr, "Query is empty\n")
		os.Exit(1)
	}

	host := flag.Arg(0)
	if host == "" {
		host = "localhost"
	}

	port := flag.Arg(1)
	if port == "" {
		port = "9042"
	}

	conn, err := getSession(host, keyspace, username, password)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Connection to %s failed): %s.\n", host+":"+port, err)
		os.Exit(1)
	}
	defer conn.Close()

	if err = conn.ExecStmt(query); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Query failed: %s.\n", err)
		os.Exit(1)
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
