package session

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
)

func Session() (*session.Session, error) {
	sess, err := session.NewSession()

	if dynamodbURL := os.Getenv("DYNAMODB_LOCAL"); dynamodbURL != "" {
		c := &aws.Config{Endpoint: &dynamodbURL}
		sess, err = session.NewSession(c)
	}

	//Check if we have credentials
	_, err = sess.Config.Credentials.Get()
	return sess, err
}
