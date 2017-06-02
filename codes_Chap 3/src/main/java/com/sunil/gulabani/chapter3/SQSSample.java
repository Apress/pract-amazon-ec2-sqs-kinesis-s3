package com.sunil.gulabani.chapter3;

import java.io.IOException;

public class SQSSample {
	private static final String AWS_ACCOUNT_ID = "YOUR_AWS_ACCOUNT_ID";
	private static final String QUEUE_NAME = "Chapter3SQSQueueName";
	private static final String DEAD_LETTER_QUEUE_NAME = "Chapter3SQSDeadLetterQueue" ;
	private static final String MESSAGE_BODY = "Hello AWS SQS!!!";

	public static void main(String[] args) throws IOException {
		String queueUrl = null;
		String deadLetterQueueUrl = null;

		SQSOperations sqsOperationsObject = new SQSOperations(AWS_ACCOUNT_ID);

		deadLetterQueueUrl = sqsOperationsObject.createQueue(DEAD_LETTER_QUEUE_NAME);

		queueUrl = sqsOperationsObject.createQueueWithAttributes(QUEUE_NAME);

		queueUrl = sqsOperationsObject.getQueueUrl(QUEUE_NAME);

		sqsOperationsObject.addPermission(queueUrl);

		sqsOperationsObject.removePermission(queueUrl);

		sqsOperationsObject.setQueueAttributes(queueUrl, DEAD_LETTER_QUEUE_NAME);

		sqsOperationsObject.getQueueAttributes(queueUrl);

		sqsOperationsObject.listQueues();

		sqsOperationsObject.listDeadLetterSourceQueues(deadLetterQueueUrl);

		sqsOperationsObject.sendMessage(queueUrl, MESSAGE_BODY);

		sqsOperationsObject.sendMessageWithAddMessageAttributes(queueUrl, MESSAGE_BODY);

		sqsOperationsObject.sendMessageWithSetMessageAttributes(queueUrl, MESSAGE_BODY);

		sqsOperationsObject.sendMessageWithTimer(queueUrl, MESSAGE_BODY);

		sqsOperationsObject.sendMessageBatch(queueUrl, MESSAGE_BODY);

		sqsOperationsObject.receiveMessageWithAck(queueUrl);

		sqsOperationsObject.receiveMessageWithChangeVisibility(queueUrl);

		sqsOperationsObject.receiveMessageWithChangeVisibilityBatch(queueUrl);

		sqsOperationsObject.purgeQueue(queueUrl);

		sqsOperationsObject.deleteQueue(queueUrl);

		sqsOperationsObject.deleteQueue(deadLetterQueueUrl);

    }
}