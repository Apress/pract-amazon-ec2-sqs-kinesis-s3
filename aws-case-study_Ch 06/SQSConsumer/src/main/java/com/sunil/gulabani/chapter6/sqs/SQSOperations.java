package com.sunil.gulabani.chapter6.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.util.StringUtils;
import com.sunil.gulabani.chapter6.core.AWSClientInitializer;

import java.util.List;

public class SQSOperations extends AWSClientInitializer {

    protected AmazonSQS amazonSQSClient;

    public SQSOperations() {
        super();
        if(!StringUtils.isNullOrEmpty(getClientConfiguration().getProxyHost())) {
            amazonSQSClient = new AmazonSQSClient(getCredentials(), getClientConfiguration());
        } else {
            amazonSQSClient = new AmazonSQSClient(getCredentials());
        }
        amazonSQSClient.setRegion(region);
    }

    public String getQueueUrl(String queueName) {
        seperator("getQueueUrl");

        GetQueueUrlResult response = amazonSQSClient.getQueueUrl(queueName);

        printObject(response);

        return response.getQueueUrl();
    }

    public List<Message> receiveMessageWithAck(String queueUrl) {

        seperator("receiveMessageWithAck");

        List<Message> messages = receiveMessage(queueUrl);
        for (Message message : messages) {
            acknowledgeMessageReceivedToQueue(message, queueUrl);
            System.out.println("-----------------------------------------");
        }
        return messages;
    }

    private List<Message> receiveMessage(String queueUrl) {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
        request.withMaxNumberOfMessages(10);
        ReceiveMessageResult response = amazonSQSClient.receiveMessage(request);
        printObject(response);
        return response.getMessages();
    }

    private void acknowledgeMessageReceivedToQueue(Message message, String queueUrl) {
        seperator("acknowledgeMessageReceivedToQueue");

        String receiptHandle = message.getReceiptHandle();

        System.out.println("Acknowledging the message: " + receiptHandle);

        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);

        DeleteMessageResult response = amazonSQSClient.deleteMessage(deleteMessageRequest);

        printObject(response);
    }
}
