package com.sunil.gulabani.chapter6.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.util.StringUtils;
import com.sunil.gulabani.chapter6.core.AWSClientInitializer;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KinesisOperations extends AWSClientInitializer {
    private AmazonKinesisClient amazonKinesisClient;

    public KinesisOperations() {
        super();
        if(!StringUtils.isNullOrEmpty(getClientConfiguration().getProxyHost())) {
            amazonKinesisClient = new AmazonKinesisClient(getCredentials(), getClientConfiguration());
        } else {
            amazonKinesisClient = new AmazonKinesisClient(getCredentials());
        }
        amazonKinesisClient.setRegion(region);
    }

    public Map<String, String> getRecords(String streamName) throws UnsupportedEncodingException {
        Map<String, String> dataMap = new HashMap<String, String>();
        GetRecordsRequest request = new GetRecordsRequest();

        List<String> shardIterators = getShardIterators(streamName, ShardIteratorType.TRIM_HORIZON);

        List<GetRecordsResult> response = new ArrayList<GetRecordsResult>();
        for(String shardIterator: shardIterators) {
            request.setShardIterator(shardIterator);
            GetRecordsResult getRecordsResult = amazonKinesisClient.getRecords(request);
            response.add(getRecordsResult);
        };
        printObject(response);

        System.out.println("-------------");
        for(GetRecordsResult result: response) {
            for(Record record: result.getRecords()) {
                String data = new String(record.getData().array(), "UTF-8");
                System.out.println(data);
                dataMap.put(record.getPartitionKey(), data);
            }
        }
        return dataMap;
    }

    private List<String> getShardIterators(String streamName, ShardIteratorType shardIteratorType) {
        List<String> shardIteratorList = new ArrayList<String>();
        List<Shard> shardList = getShards(streamName);
        for (Shard shard: shardList) {
            GetShardIteratorResult shardIteratorResult = getShardIteratorResult(streamName, shardIteratorType, shard);
            printObject(shardIteratorResult);
            shardIteratorList.add(shardIteratorResult.getShardIterator());
        };
        return shardIteratorList;
    }

    private List<Shard> getShards(String streamName) {
        DescribeStreamResult describeStreamResult = describeStream(streamName);
        return describeStreamResult.getStreamDescription().getShards();
    }

    private DescribeStreamResult describeStream(String streamName) {
        DescribeStreamRequest request = new DescribeStreamRequest();

        request.setStreamName(streamName);

        DescribeStreamResult response = amazonKinesisClient.describeStream(request);

        printObject(response);

        return response;
    }

    private GetShardIteratorResult getShardIteratorResult(String streamName, ShardIteratorType shardIteratorType, Shard shard) {
        GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest();
        shardIteratorRequest.setShardId(shard.getShardId());
        shardIteratorRequest.setShardIteratorType(shardIteratorType);
        shardIteratorRequest.setStreamName(streamName);

        return amazonKinesisClient.getShardIterator(shardIteratorRequest);
    }
}
