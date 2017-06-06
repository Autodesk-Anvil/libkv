package streams

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
)

func getSession() (*session.Session, error) {
	sess, err := session.NewSession()

	if dynamodbURL := os.Getenv("DYNAMODB_LOCAL"); dynamodbURL != "" {
		fmt.Printf("\nDYNAMODB_LOCAL is set to %v\n", dynamodbURL)
		c := &aws.Config{
			Endpoint: &dynamodbURL,
		}
		//Create a Session with a custom region
		sess, err = session.NewSession(c)
	}

	// Fail early, if no credentials can be found
	_, err = sess.Config.Credentials.Get()
	return sess, err
}
