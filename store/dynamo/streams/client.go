package streams

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"os"
)

var RestDBClient *Client
var StreamDBClient *StreamClient
var Tablename string = "traefik"
// var client *dynamodb.Client


// Client is a wrapper around the DynamoDB client
// and also holds the table to lookup key value pairs from
type Client struct {
	client *dynamodb.DynamoDB
	table  string
}

func getSession() (*session.Session, error) {
	sess, err := session.NewSession()

	if dynamodbURL := os.Getenv("DYNAMODB_LOCAL"); dynamodbURL != "" {
		fmt.Println("DYNAMODB_LOCAL is set to %v", dynamodbURL)
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

// NewDynamoDBClient returns an *dynamodb.Client with a connection to the region
// configured via the AWS_REGION environment variable.
// It returns an error if the connection cannot be made or the table does not exist.
func NewDynamoDBClient(table string) (*Client, error) {
	
	// Create Session
	sess, err := getSession()
	if err != nil {
		return nil, err
	}
	d := dynamodb.New(sess)

	// Check if the table exists
	_, err = d.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return nil, err
	}
	return &Client{d, table}, nil
}

func (c *Client) GetTables() error {

	op, err := c.client.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return err
	} else {
		fmt.Printf("tables : %+v", op)
	}
	return nil
}

func (c *Client) Scan() {
	params := &dynamodb.ScanInput{
		TableName: aws.String(c.table), // Required
		AttributesToGet: []*string{
			aws.String("Artist"),     // Required
			aws.String("SongTitle"),  // Required
			aws.String("AlbumTitle"), // Required
			// More values...
		},
	}
	resp, err := c.client.Scan(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	fmt.Println(resp)
}

func (c *Client) Query() {
	ss := "Acme Band"
	var params = &dynamodb.QueryInput{
		TableName: aws.String(c.table),
		KeyConditions: map[string]*dynamodb.Condition{
			"Artist": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: &ss,
					},
				},
			},
		},
	}

	resp, err := c.client.Query(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	fmt.Printf("Query resp: %v", resp)
}

// func (c *Client) Update() error {
// 	ss := "Acme Band"
// 	params := &dynamodb.UpdateItemInput{
//     	Key: map[string]*dynamodb.AttributeValue{ // Required
//         	"Key": { // Required
// 	            S:    aws.String("StringAttributeValue"),
//             },
//         },
//     	TableName: aws.String("Music"), // Required
//     	AttributeUpdates: map[string]*dynamodb.AttributeValueUpdate{
//         "Key": { // Required
//             Action: aws.String("PUT"),
//             Value: &dynamodb.AttributeValue{
//                 S:    aws.String("StringAttributeValue"),
//             },
//         },
//         // More values...
//     },
//     },
// }

// 	resp, err := c.client.Query(params)

// 	if err != nil {
// 		// Print the error, cast err to awserr.Error to get the Code and
// 		// Message from an error.
// 		fmt.Println(err.Error())
// 		return nil
// 	}

// 	// Pretty-print the response data.
// 	fmt.Printf("Query resp: %v", resp)
// 	return nil
// }

// WatchPrefix is not implemented
func (c *Client) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	<-stopChan
	return 0, nil
}

func init() {
	var err error
	RestDBClient, err = NewDynamoDBClient(Tablename)
	if err != nil {
		log.Printf("error creating client: %v", err)
		panic("could not create client")
	}
	StreamDBClient, err = NewDynamoDBStreamClient(Tablename)
	if err != nil {
		log.Printf("error creating StreamClient: %v", err)
		panic("could not create client")
	}

}

