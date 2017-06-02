package com.sunil.gulabani.chapter3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.util.StringUtils;
import com.sunil.gulabani.chapter3.core.AWSClientInitializer;

public class SQSOperations extends AWSClientInitializer {
	
	protected AmazonSQS amazonSQSClient;
	private String awsUserAccountId;
	
	public SQSOperations(String awsUserAccountId) {
		super();
		if(!StringUtils.isNullOrEmpty(getClientConfiguration().getProxyHost())) {
			amazonSQSClient = new AmazonSQSClient(getCredentials(), getClientConfiguration());
		} else {
			amazonSQSClient = new AmazonSQSClient(getCredentials());
		}
		amazonSQSClient.setRegion(region);
		this.awsUserAccountId = awsUserAccountId;
	}
	
	public String createQueue(String queueName) {
		seperator("createQueue");

        CreateQueueRequest request = new CreateQueueRequest(queueName);

		CreateQueueResult response = amazonSQSClient.createQueue(request);

		printObject(response, "createQueue");

        return response.getQueueUrl();
	}

	public String createQueueWithAttributes(String queueName) {
		seperator("createQueueWithAttributes");

		String queueNameTargetARN = "arn:aws:sqs:" + region.getName() + ":" + awsUserAccountId + ":" + queueName;
		String queueNameDefaultPolicyId = queueNameTargetARN + "/SQSDefaultPolicy";

        CreateQueueRequest request = new CreateQueueRequest(queueName);

		request.addAttributesEntry("DelaySeconds", "0");
		request.addAttributesEntry("MaximumMessageSize", "262144");
		request.addAttributesEntry("MessageRetentionPeriod", "1209600");
		request.addAttributesEntry("ReceiveMessageWaitTimeSeconds", "0");
		request.addAttributesEntry("VisibilityTimeout", "30");
		request.addAttributesEntry("Policy", "{" +
				"\"Version\": \"2012-10-17\"," +
				"\"Id\": \"" + queueNameDefaultPolicyId + "\"," +
				"\"Statement\": [" +
				"	{" +
				"		\"Sid\": \"Sid1465389499769\"," +
				"		\"Effect\": \"Allow\"," +
				"		\"Principal\": \"*\"," +
				"		\"Action\": \"SQS:*\"," +
				"		\"Resource\": \"" + queueNameTargetARN + "\"" +
				"	}" +
				"]}");

		CreateQueueResult response = amazonSQSClient.createQueue(request);

		printObject(response, "createQueueWithAttributes");

		return response.getQueueUrl();
	}

	public void addPermission(String queueUrl) {
		seperator("addPermission");

		AddPermissionRequest request = new AddPermissionRequest();
		
		request.setQueueUrl(queueUrl);
		
		request.setActions(getActions());
		
		request.setAWSAccountIds(getAWSAccountIds());
		
		request.setLabel("Chapter3SQSPermission");

		AddPermissionResult response = amazonSQSClient.addPermission(request);

		printObject(response, "addPermission");
	}

	public void removePermission(String queueUrl) {
		seperator("removePermission");
		
		RemovePermissionRequest request = new RemovePermissionRequest(queueUrl, "Chapter3SQSPermission");
		
		RemovePermissionResult response = amazonSQSClient.removePermission(request);

		printObject(response, "removePermission");
	}
	
	private List<String> getAWSAccountIds() {
		List<String> awsAccountIdsList = new ArrayList<String>();
		awsAccountIdsList.add(awsUserAccountId);
		return awsAccountIdsList;
	}
	
	private List<String> getActions() {
		List<String> actionsList = new ArrayList<String>();
//		actionsList.add("*");
		actionsList.add("SendMessage");
		actionsList.add("ReceiveMessage");
		actionsList.add("DeleteMessage");
		actionsList.add("ChangeMessageVisibility");
		actionsList.add("GetQueueAttributes");
		actionsList.add("GetQueueUrl");
		return actionsList;
	}
	
	public void setQueueAttributes(String queueUrl, String deadLetterQueueName) {
		seperator("setQueueAttributes");

		SetQueueAttributesRequest request = new SetQueueAttributesRequest(queueUrl, createQueueAttributes(getNameFromUrl(queueUrl), deadLetterQueueName));
		
		SetQueueAttributesResult response = amazonSQSClient.setQueueAttributes(request);

		printObject(response, "setQueueAttributes");
	}
	
	private Map<String, String> createQueueAttributes(String queueName, String deadLetterQueueName) {
		Map<String, String> attributes = new HashMap<String, String>();
		String queueNameTargetARN = "arn:aws:sqs:" + region.getName() + ":" + awsUserAccountId + ":" + queueName;
		String queueNameDefaultPolicyId = queueNameTargetARN + "/SQSDefaultPolicy";
		String deadLetterTargetARN = "arn:aws:sqs:" + region.getName() + ":" + awsUserAccountId + ":" + deadLetterQueueName;

		attributes.put("DelaySeconds", "0");
        attributes.put("MaximumMessageSize", "262144");
        attributes.put("MessageRetentionPeriod", "1209600");
        attributes.put("ReceiveMessageWaitTimeSeconds", "0");
        attributes.put("VisibilityTimeout", "30");
        attributes.put("Policy", "{" +
				"\"Version\": \"2012-10-17\"," +
				"\"Id\": \"" + queueNameDefaultPolicyId + "\"," +
				"\"Statement\": [" +
				"	{" +
				"		\"Sid\": \"Sid1465389499769\"," +
				"		\"Effect\": \"Allow\"," +
				"		\"Principal\": \"*\"," +
				"		\"Action\": \"SQS:*\"," +
				"		\"Resource\": \"" + queueNameTargetARN + "\"" +
				"	}" +
				"]}");

		attributes.put("RedrivePolicy", "{\"maxReceiveCount\":\"10\", \"deadLetterTargetArn\":\"" + deadLetterTargetARN + "\"}");

		return attributes;
	}

	public void getQueueAttributes(String queueUrl) {
		seperator("getQueueAttributes");

		GetQueueAttributesRequest request = new GetQueueAttributesRequest();
		request.setQueueUrl(queueUrl);
		request.setAttributeNames(getAttributeNames());

		GetQueueAttributesResult response = amazonSQSClient.getQueueAttributes(request);

		printObject(response, "getQueueAttributes");
	}
	
	private List<String> getAttributeNames() {
		List<String> attributeNames = new ArrayList<String>();
		
		attributeNames.add("DelaySeconds");
		attributeNames.add("MaximumMessageSize");
		attributeNames.add("MessageRetentionPeriod");
		attributeNames.add("ReceiveMessageWaitTimeSeconds");
		attributeNames.add("VisibilityTimeout");
		attributeNames.add("Policy");
        
        return attributeNames;
	}

	public void listDeadLetterSourceQueues(String queueUrl) {
		seperator("listDeadLetterSourceQueues");
		ListDeadLetterSourceQueuesRequest request = new ListDeadLetterSourceQueuesRequest();
		
		request.setQueueUrl(queueUrl);
		
		ListDeadLetterSourceQueuesResult response = amazonSQSClient.listDeadLetterSourceQueues(request);

		printObject(response, "listDeadLetterSourceQueues");
	}
	
	public void listQueues() {
		seperator("listQueues");
		
		ListQueuesResult response = amazonSQSClient.listQueues();

		printObject(response, "listQueues");
	}
	
	public String getQueueUrl(String queueName) {
		seperator("getQueueUrl");

		GetQueueUrlResult response = amazonSQSClient.getQueueUrl(queueName);

		printObject(response, "getQueueUrl");

		return response.getQueueUrl();
	}
	
	public void sendMessage(String queueUrl, String body) {
		seperator("sendMessage");
		
		SendMessageRequest request = new SendMessageRequest(queueUrl, body);
		
		sendMessage(request, "sendMessage");
	}
	
	public void sendMessageWithTimer(String queueUrl, String body) {
		seperator("sendMessageWithTimer");
		
		SendMessageRequest request = new SendMessageRequest(queueUrl, body);
		request.setDelaySeconds(30);
		
		sendMessage(request, "sendMessageWithTimer");
	}
	
	public void sendMessageWithSetMessageAttributes(String queueUrl, String body) {
		seperator("sendMessageWithSetMessageAttributes");
		
		SendMessageRequest request = new SendMessageRequest(queueUrl, body);
		request.setMessageAttributes(createMessageAttributeValuesMap());
		
		sendMessage(request, "sendMessageWithSetMessageAttributes");
	}
	
	public void sendMessageWithAddMessageAttributes(String queueUrl, String body) {
		seperator("sendMessageWithAddMessageAttributes");
		
		SendMessageRequest request = new SendMessageRequest(queueUrl, body);
		
		for(int i = 0 ; i < 5 ; i++) {
			request.addMessageAttributesEntry("Key-" + i, createMessageAttributeValue("Value-" + i));
		}
		
		sendMessage(request, "sendMessageWithAddMessageAttributes");
	}

	private void sendMessage(SendMessageRequest request, String operationType) {
		SendMessageResult response = amazonSQSClient.sendMessage(request);

		printObject(response, operationType);
	}
	
	public void sendMessageBatch(String queueUrl, String body) {
		seperator("sendMessageBatch");
		
		SendMessageBatchRequest request = new SendMessageBatchRequest(queueUrl);

		request.setEntries(createSendMessageBatchRequestEntry(body));

		SendMessageBatchResult response = amazonSQSClient.sendMessageBatch(request);

		printObject(response, "sendMessageBatch");
	}

	private List<SendMessageBatchRequestEntry> createSendMessageBatchRequestEntry(String body) {
		List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();

		SendMessageBatchRequestEntry sendMessageBatchRequestEntry;
		for(int i = 0; i < 5; i++) {
			sendMessageBatchRequestEntry = new SendMessageBatchRequestEntry();
			sendMessageBatchRequestEntry.setId(UUID.randomUUID().toString());
			sendMessageBatchRequestEntry.setMessageBody(body + "-" + i);
//			sendMessageBatchRequestEntry.setDelaySeconds(30);
			entries.add(sendMessageBatchRequestEntry);
		}
		
		return entries;
	}
	
	private Map<String, MessageAttributeValue> createMessageAttributeValuesMap() {
		Map<String, MessageAttributeValue> messageAttributeValuesMap = new HashMap<String, MessageAttributeValue>();
		
		for(int i = 0 ; i < 5 ; i++) {
			messageAttributeValuesMap.put("Key-" + i, createMessageAttributeValue("Value-" + i));
		}
		
		return messageAttributeValuesMap;
	}
	
	private MessageAttributeValue createMessageAttributeValue(String value) {
		MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
		
		messageAttributeValue.setStringValue(value);
		
		messageAttributeValue.setDataType("String");
		
		return messageAttributeValue;
	}
	
	public void receiveMessageWithAck(String queueUrl) {
		
		seperator("receiveMessageWithAck");
		
		List<Message> messages = receiveMessage(queueUrl, "receiveMessageWithAck");
        for (Message message : messages) {
            acknowledgeMessageReceivedToQueue(message, queueUrl);
            System.out.println("-----------------------------------------");
        }
	}
	
	public void receiveMessageWithChangeVisibility(String queueUrl) {
		
		seperator("receiveMessageWithChangeVisibility");
		
		List<Message> messages = receiveMessage(queueUrl, "receiveMessageWithChangeVisibility");
        for (Message message : messages) {
            changeMessageVisibility(queueUrl, message.getReceiptHandle(), 10);
            System.out.println("-----------------------------------------");
        }
	}
	
	public void receiveMessageWithChangeVisibilityBatch(String queueUrl) {
		seperator("receiveMessageWithChangeVisibilityBatch");

		List<Message> messages = receiveMessage(queueUrl, "receiveMessageWithChangeVisibilityBatch");

        changeMessageVisibilityBatch(queueUrl, messages);
	}
	
	private List<Message> receiveMessage(String queueUrl, String operationType) {
		ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
		request.withMaxNumberOfMessages(10);
		ReceiveMessageResult response = amazonSQSClient.receiveMessage(request);
		printObject(response, operationType);
		return response.getMessages();
	}

	private void acknowledgeMessageReceivedToQueue(Message message, String queueUrl) {
		seperator("acknowledgeMessageReceivedToQueue");

		String receiptHandle = message.getReceiptHandle();
		
		System.out.println("Acknowledging the message: " + receiptHandle);

		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);
		
		DeleteMessageResult response = amazonSQSClient.deleteMessage(deleteMessageRequest);

		printObject(response, "acknowledgeMessageReceivedToQueue");
	}

	private void changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout) {
		seperator("changeMessageVisibility");
		ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, visibilityTimeout);
		
		ChangeMessageVisibilityResult response = amazonSQSClient.changeMessageVisibility(request);

		printObject(response, "changeMessageVisibility");
		
	}
	
	private void changeMessageVisibilityBatch(String queueUrl, List<Message> messages) {
		seperator("changeMessageVisibilityBatch");

		ChangeMessageVisibilityBatchRequest request = new ChangeMessageVisibilityBatchRequest();
		
		request.setQueueUrl(queueUrl);
		
		List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();
		
		ChangeMessageVisibilityBatchRequestEntry entry;
		
		for(Message message: messages) {
			
			entry = new ChangeMessageVisibilityBatchRequestEntry(UUID.randomUUID().toString(), message.getReceiptHandle());
			
			entry.setVisibilityTimeout(10);
			
			entries.add(entry);
			
		}
		
		request.setEntries(entries);
		
		ChangeMessageVisibilityBatchResult response = amazonSQSClient.changeMessageVisibilityBatch(request);

		printObject(response, "changeMessageVisibilityBatch");
	}
	
	public void purgeQueue(String queueUrl) {
		seperator("purgeQueue");

		PurgeQueueRequest request = new PurgeQueueRequest(queueUrl);

		PurgeQueueResult response = amazonSQSClient.purgeQueue(request);

		printObject(response, "purgeQueue");
	}
	
	public void deleteQueue(String queueUrl) {
		seperator("deleteQueue");

		DeleteQueueRequest request = new DeleteQueueRequest(queueUrl);

		DeleteQueueResult response = amazonSQSClient.deleteQueue(request);

		printObject(response, "deleteQueue");
	}
	
	private String getNameFromUrl(String queueUrl) {
		return queueUrl.substring(queueUrl.lastIndexOf("/") + 1);
	}
}
