package streams

import (
	"errors"
	"fmt"
	"github.com/autodesk-anvil/libkv/store/dynamo/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"log"
	"math/big"
)

var (
	ErrNoStreamFound = errors.New("No streams found")
)

//Dynamodb stream client wrapper exposes a channel to give out the updates for a given table
//that occured after specified sequence number.
type StreamClient struct {
	client         *dynamodbstreams.DynamoDBStreams
	table          string
	streams        []string
	Ch             chan *dynamodbstreams.Record
	sequenceNumber *big.Int
}

// NewDynamoDBStreamClient returns an *dynamodb.Client with a connection to the region
// configured via the AWS_REGION environment variable.
// It returns an error if the connection cannot be made or the table does not exist.
func NewDynamoDBStreamClient(table string, sequenceNumber string, done <-chan struct{}) (*StreamClient, error) {

	//create a dynamodb client
	sess, err := session.Session()
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

func (c *StreamClient) processStreams(done <-chan struct{}) {
	for _, stream := range c.streams {
		c.processStream(stream, done)
		//todo: close the output channel if all streams are processed
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

	//get shards
	var shards []*dynamodbstreams.Shard = describeStreamInputResp.StreamDescription.Shards

	for _, shard := range shards {
		go c.processShard(shard, stream, done)
		//todo: notify when all the shards of a stream are processed. Possibly close channel
	}
	go func() {
		select {
		case <-done:
			close(c.Ch)
		}
	}()
}

func (c *StreamClient) processShard(shard *dynamodbstreams.Shard, stream string, done <-chan struct{}) {

	shardName := *shard.ShardId
	if len(shardName) > 10 {
		shardName = shardName[len(shardName)-3:]
	}
	//log.Printf("processShard start(%v)\n", shardName)

	maxSequenceNumber, err2 := toBigInt(shard.SequenceNumberRange.EndingSequenceNumber)
	if err2 == nil {
		if c.sequenceNumber.Cmp(maxSequenceNumber) == 1 {
			//these events are too old
			return
		}
	}

	getShardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           aws.String(*shard.ShardId),                                    // Required
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber), //aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber), // Required
		SequenceNumber:    shard.SequenceNumberRange.StartingSequenceNumber,
		StreamArn:         aws.String(stream), // Required
	}

	getShardIteratorInputResp, err := c.client.GetShardIterator(getShardIteratorInput)
	if err != nil {
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
			//fmt.Printf("\nerror processing(%v) records : err: %v\n", shardName, err.Error())
			break
		}

		for _, v := range getRecordsInputResp.Records {
			sn, err := toBigInt(v.Dynamodb.SequenceNumber)

			//condition: the record sequence number sn is not less than c.sequenceNumber
			if err == nil && sn.Cmp(c.sequenceNumber) != -1 {
				//log.Printf("processShard event(%v)", *v.Dynamodb.Keys["Key"].S)
				c.Ch <- v
			}
		}

		select {
		case <-done:
			//fmt.Printf("\nprocessShard canceled(%v)\n", shardName)
			return
		default:
		}
		iter = getRecordsInputResp.NextShardIterator
	}
	//log.Printf("processShard done(%v)\n", shardName)
}

//Select the streams for given table
func selectStreams(client *dynamodbstreams.DynamoDBStreams, table string) ([]string, error) {

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

//Utility function to convert from AWS sequence number to an Integer or arbitrary precesion
func toBigInt(s *string) (*big.Int, error) {
	if s == nil {
		return nil, fmt.Errorf("Invalid number")
	}
	var i = new(big.Int)
	_, err := fmt.Sscan(*s, i)
	return i, err
}
