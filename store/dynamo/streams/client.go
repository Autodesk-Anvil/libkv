package streams

import (
	"errors"
	"fmt"
	"github.com/autodesk-anvil/libkv/store/dynamo/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"log"
	"math/big"
	"time"
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
	//timestamp		 *time.Time
	MaxSquenceNumber *big.Int
}

// NewDynamoDBStreamClient returns an *dynamodb.Client with a connection to the region
// configured via the AWS_REGION environment variable.
// It returns an error if the connection cannot be made or the table does not exist.
func NewDynamoDBStreamClient(table string, sequenceNumber string /*timestamp *time.Time,*/, done <-chan struct{}) (*StreamClient, error) {
	log.Printf("NewDynamoDBStreamClient(%v, %v)", table, sequenceNumber)
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

	var sn *big.Int
	log.Printf("\nNewDynamoDBStreamClient.sequenceNumber(%v is %v long)\n", sequenceNumber, len(sequenceNumber))

	if len(sequenceNumber) != 0 {
		sn, err = toBigInt(&sequenceNumber)
		if err != nil {
			log.Printf("bad sequence number(%v) error %v", sequenceNumber, err)
			return nil, err
		}
	}

	c := &StreamClient{client, table, streams, make(chan *dynamodbstreams.Record, 100000), sn, new(big.Int)}

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

//set the Max sequence number found so far
func (c *StreamClient) setMaxSequenceNumber(sn *big.Int) {
	if sn.Cmp(c.MaxSquenceNumber) != -1 {
		c.MaxSquenceNumber = sn
	}
}

func (c *StreamClient) processShard(shard *dynamodbstreams.Shard, stream string, done <-chan struct{}) {

	shardName := *shard.ShardId
	if len(shardName) > 10 {
		shardName = shardName[len(shardName)-3:]
	}

	getShardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:   aws.String(*shard.ShardId), // Required
		StreamArn: aws.String(stream),         // Required
	}

	if c.sequenceNumber != nil {
		maxSequenceNumber, err2 := toBigInt(shard.SequenceNumberRange.EndingSequenceNumber)
		if err2 == nil {
			if c.sequenceNumber.Cmp(maxSequenceNumber) == 1 {
				//these events are too old
				return
			} else {
				c.setMaxSequenceNumber(maxSequenceNumber)
			}
		}

		fmt.Println("using StartingSequenceNumber")
		getShardIteratorInput.ShardIteratorType = aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber) 
		getShardIteratorInput.SequenceNumber = shard.SequenceNumberRange.StartingSequenceNumber

	} else {
		if shard.SequenceNumberRange.EndingSequenceNumber != nil {
			fmt.Println("shard already closed...returning")
			return
		}
		fmt.Println("using latest")
		getShardIteratorInput.ShardIteratorType = aws.String(dynamodbstreams.ShardIteratorTypeLatest)
	}
	log.Printf("processShard start(%+v)\n", shard)
	log.Printf("input(%+v)\n", getShardIteratorInput)

	for {
		getShardIteratorInputResp, err := c.client.GetShardIterator(getShardIteratorInput)
		if err != nil || getShardIteratorInputResp == nil {
			log.Printf("\nerror processing(%v): err: %v\n", shardName, err.Error())
			return
		}
		initIter := getShardIteratorInputResp.ShardIterator
		iter := initIter
		log.Printf("got first iterator (%v)", iter)
		for iter != nil {
			//log.Printf("working on iterator(%v)", iter)
			//get records
			getRecordsInput := &dynamodbstreams.GetRecordsInput{
				ShardIterator: aws.String(*iter), // Required
			}
			//log.Printf("getRecordsInput(%v)", getRecordsInput)
			getRecordsInputResp, err := c.client.GetRecords(getRecordsInput)

			if err != nil {
				log.Printf("\nerror processing(%v) records : err: %v\n", shardName, err.Error())
				//todo: this happens in case of empty stream (new table or 24 hour non-updates). todo introduce bakcoff
				//todo(sabder): make sure when we break and repeat over this shard, we don't collect records already collected.
				//idealy that should be the case.
				<-time.After(2 * time.Second)
				break
			}

			for _, v := range getRecordsInputResp.Records {
				//log.Printf("gshard(%v)-> record(%v)", shardName, v)

				sn, err := toBigInt(v.Dynamodb.SequenceNumber)
				log.Printf("got record sequenceNumber(%+v)", sn)

				//condition: the record sequence number sn is not less than c.sequenceNumber
				if err == nil {
					//todo: we are dropping the records here. investigate
					if c.sequenceNumber == nil || sn.Cmp(c.sequenceNumber) != -1 {
						log.Printf("sending the record with sn(%+v)", sn)
						c.Ch <- v
					}
					c.setMaxSequenceNumber(sn)
				}

			}

			select {
			case <-done:
				log.Printf("\nprocessShard canceled(%v)\n", shardName)
				return
			default:
			}
			iter = getRecordsInputResp.NextShardIterator
			//log.Printf("got next iterator (%v)", iter)
		}
	}
	log.Printf("processShard done(%v)\n", shardName)
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
