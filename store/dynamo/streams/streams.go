package streams

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"log"
	"time"
)

// Client is a wrapper around the DynamoDB client
// and also holds the table to lookup key value pairs from
type StreamClient struct {
	client *dynamodbstreams.DynamoDBStreams
	table  string
	stream string
	ch 		chan *dynamodbstreams.Record
	streaming bool
}

// NewDynamoDBClient returns an *dynamodb.Client with a connection to the region
// configured via the AWS_REGION environment variable.
// It returns an error if the connection cannot be made or the table does not exist.
func NewDynamoDBStreamClient(table string) (*StreamClient, error) {

	sess, err := getSession()
	if err != nil {
		return nil, err
	}
	
	// Fail early, if no credentials can be found
	_, err = sess.Config.Credentials.Get()
	if err != nil {
		return nil, err
	}

	client := dynamodbstreams.New(sess)

	params := &dynamodbstreams.ListStreamsInput{
		TableName: aws.String(table),
	}
	resp, err := client.ListStreams(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return nil, err
	}
	// Pretty-print the response data.
	fmt.Println(resp)

	var arn string
	if len(resp.Streams) > 0 {
		stream := resp.Streams[0]
		//now describe the stream
		arn = *stream.StreamArn
	}

	c := &StreamClient{client, table, arn, make(chan *dynamodbstreams.Record, 1000), false}
	go c.GetRecords()
	return c, nil
}

func (c *StreamClient) Watch()  <-chan *dynamodbstreams.Record {
	c.streaming = true
	return c.ch
}


func (c *StreamClient) StopWatch(){
	c.streaming = false
}

func (c *StreamClient) GetRecords() {

	for {
		if !c.streaming {
			<-time.After(time.Second)
			continue
		}
		log.Println("infinite loop ...")
		describeStreamInput := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(c.stream), // Required
		}
		describeStreamInputResp, err := c.client.DescribeStream(describeStreamInput)

		if err != nil {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
			return
		}

		fmt.Printf("describestream: %+v", describeStreamInputResp)
		//get shards
		var shards []*dynamodbstreams.Shard
		shards = describeStreamInputResp.StreamDescription.Shards

		log.Println("looping over shards ...")
		for i := 0; i < len(shards); i++ {
			log.Printf("processing shard %v\n", *shards[i].ShardId)
			getShardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
				ShardId:           aws.String(*shards[i].ShardId),                      // Required
				//ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeLatest), // Required
				ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber), // Required
				//ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeLatest), // Required
				SequenceNumber: shards[i].SequenceNumberRange.StartingSequenceNumber,
				StreamArn: aws.String(c.stream), // Required
			}

			// getShardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
			// 	ShardId:           aws.String(*shards[i].ShardId),                      // Required
			// 	ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeLatest), // Required
			// 	//ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber), // Required
			// 	//ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeLatest), // Required
			// 	//StartingSequenceNumber: "000000000000000000002",
			// 	StreamArn: aws.String(c.stream), // Required
			// }

			//log.Printf("getting shardIterator for next shard %v", getShardIteratorInput.ShardId)
			getShardIteratorInputResp, err := c.client.GetShardIterator(getShardIteratorInput)

			if err != nil {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
				return
			}

			iter := getShardIteratorInputResp.ShardIterator
			for iter != nil {
				// Pretty-print the response data.
				// fmt.Printf("shard iterator %v\n", *iter)
				//get records
				getRecordsInput := &dynamodbstreams.GetRecordsInput{
					ShardIterator: aws.String(*iter), // Required
				}
				getRecordsInputResp, err := c.client.GetRecords(getRecordsInput)
				if err != nil {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					fmt.Println(err.Error())
					break
				}

				for _, v := range getRecordsInputResp.Records {
					fmt.Println("<- sending record")
					c.ch <- v
				}
				if !c.streaming {
					break
				}
				iter = getRecordsInputResp.NextShardIterator
				<-time.After(time.Second)
			}

		}

	}

}

// WatchPrefix is not implemented
func (c *StreamClient) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	<-stopChan
	return 0, nil
}
