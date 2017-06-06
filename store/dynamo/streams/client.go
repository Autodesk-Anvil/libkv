package streams

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"log"
	"errors"
	"math/big"

)
/*
	It is recommended that we do not have more than a couple streamsclient.
*/

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Dynamodb
	ErrNoStreamFound = errors.New("No streams found")
)

// Client is a wrapper around the DynamoDB client
// and also holds the table to lookup key value pairs from
type StreamClient struct {
	client 			*dynamodbstreams.DynamoDBStreams
	table  			string
	streams 		[]string
	Ch 				chan *dynamodbstreams.Record
	sequenceNumber 	*big.Int
}

func toBigInt(s *string) (*big.Int, error) {
	if s == nil {
		return nil, fmt.Errorf("Invalid number")
	}
	var i = new(big.Int)
	_, err := fmt.Sscan(*s, i)
	return i, err
}

// NewDynamoDBStreamClient returns an *dynamodb.Client with a connection to the region
// configured via the AWS_REGION environment variable.
// It returns an error if the connection cannot be made or the table does not exist.
func NewDynamoDBStreamClient(table string, sequenceNumber string, done <-chan struct {}) (*StreamClient, error) {

	//create a dynamodb client
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


	//Select streams
	streams, err := selectStreams(client, table)
	if err != nil || len(streams) < 1 {
		return nil, ErrNoStreamFound
	}

	sn, err := toBigInt(&sequenceNumber)
	if err != nil {
		log.Printf("bad sequence number(%v) error %v", sequenceNumber, err)
		return nil, err
	}

	c := &StreamClient{client, table, streams, make(chan *dynamodbstreams.Record, 100000), sn}
	
	//start pulling from the stream
	c.processStreams(done)
	return c, nil
}

func selectStreams(client *dynamodbstreams.DynamoDBStreams, table string) ([]string, error){

	params := &dynamodbstreams.ListStreamsInput{
		TableName: aws.String(table),
	}
	resp, err := client.ListStreams(params)

	if err != nil || len(resp.Streams) == 0 {
		return nil, fmt.Errorf("streams not found")
	}
	streams := make([]string, 0)
	for _, v := range resp.Streams {
		streams = append(streams, *v.StreamArn)
	}
	return streams, nil
}

func (c *StreamClient) processStreams(done <-chan struct{}) {
	for _, stream := range c.streams {
		c.processStream(stream, done)
	}
}

//set up pipeline that can be cancelled
func (c *StreamClient) processStream(stream string, done <-chan struct{}) {

	describeStreamInput := &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(stream), // Required
	}
	describeStreamInputResp, err := c.client.DescribeStream(describeStreamInput)

	if err != nil {
		fmt.Println(err.Error())
		return
	} else {
		//log.Printf("describeStreamInputResp: %+v", describeStreamInputResp)
	}

	//get shards //todo(do we need to keep scanning for shards?)
	var shards []*dynamodbstreams.Shard = describeStreamInputResp.StreamDescription.Shards
	
	for _, shard := range shards {
		go c.processShard(shard, stream, done)
	}
	go func(){
		select{
			case <-done:
				log.Println("\nclosing c.Ch")
				close(c.Ch)
		}
	}()
}

func (c *StreamClient) processShard(shard *dynamodbstreams.Shard, stream string, done <-chan struct{}) {
	shardName := *shard.ShardId
	if len(shardName) > 10 {
		shardName = shardName[len(shardName)-3:]
	}
	//log.Printf("processing shard %v\n", shardName)

	maxSequenceNumber, err2 := toBigInt(shard.SequenceNumberRange.EndingSequenceNumber)
	if err2 == nil {
		//x > y
		if c.sequenceNumber.Cmp(maxSequenceNumber) == 1 {
			//these events are too old
			return
		}
	}

	getShardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           	aws.String(*shard.ShardId),                      // Required
		ShardIteratorType: 	aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber), //aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber), // Required
		SequenceNumber: 	shard.SequenceNumberRange.StartingSequenceNumber,
		StreamArn: 			aws.String(stream), // Required
	}
	//log.Printf("GetShardIterator(%+v)", getShardIteratorInput)
	getShardIteratorInputResp, err := c.client.GetShardIterator(getShardIteratorInput)		
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		//fmt.Printf("\nerror processing(%v): err: %v\n", shardName, err.Error())
		return
	}
	iter := getShardIteratorInputResp.ShardIterator

	for iter != nil {
		
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
			//the record sequence number sn is not less than c.sequenceNumber 
			sn, err := toBigInt(v.Dynamodb.SequenceNumber)
			if err == nil && sn.Cmp(c.sequenceNumber) != -1 {
				//log.Printf("dynamodbstreams event(%v)", v)
				c.Ch <- v
			} else {
				//log.Printf("rejecting event:sequence number %v out of range", *v.Dynamodb.SequenceNumber)
			}
		}
		
		select {
		case <- done:
			fmt.Printf("\nprocessShard(%v) canceled \n", shardName)
			return
		default:
		}
		iter = getRecordsInputResp.NextShardIterator
	}
	//log.Printf("done processing shardId %v", shardName)
}

