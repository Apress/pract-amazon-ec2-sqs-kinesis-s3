package com.sunil.gulabani.chapter6.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.util.StringUtils;
import com.sunil.gulabani.chapter6.core.AWSClientInitializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

    public String putRecord(String streamName, String partitionKey, byte[] data) {
        PutRecordRequest request = new PutRecordRequest();
        request.setStreamName(streamName);
        request.setPartitionKey(partitionKey);
        request.setData(ByteBuffer.wrap(data));

        PutRecordResult response = amazonKinesisClient.putRecord(request);

        printObject(response);
        return response.getSequenceNumber();
    }
}
