package co.sqs.demo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SQSApplication
{
    private static final AWSCredentials credentials;

    static {
        // put your accesskey and secretkey here
        credentials = new BasicAWSCredentials(
                "ACCESS_KEY",
                "SECRET-KEY"
        );
    }

    public static void main(String[] args) {

        // Set up the client
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.AP_SOUTH_1)
                .build();

        // Create a standard queue

        CreateQueueRequest createStandardQueueRequest = new CreateQueueRequest("javasdkdemo-queue");
        String standardQueueUrl = sqs.createQueue(createStandardQueueRequest)
                .getQueueUrl();

        System.out.println(standardQueueUrl);

        // Create a fifo queue

        Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("FifoQueue", "true");
        queueAttributes.put("ContentBasedDeduplication", "true");

        CreateQueueRequest createFifoQueueRequest = new CreateQueueRequest("javasdkdemo-queue.fifo").withAttributes(queueAttributes);
        String fifoQueueUrl = sqs.createQueue(createFifoQueueRequest)
                .getQueueUrl();

        System.out.println(fifoQueueUrl);

        // Set up a dead letter queue

        String deadLetterQueueUrl = sqs.createQueue("javasdkdemo-queue-dead-letter-queue")
                .getQueueUrl();

        GetQueueAttributesResult deadLetterQueueAttributes = sqs.getQueueAttributes(new GetQueueAttributesRequest(deadLetterQueueUrl).withAttributeNames("QueueArn"));

        String deadLetterQueueARN = deadLetterQueueAttributes.getAttributes()
                .get("QueueArn");

        SetQueueAttributesRequest queueAttributesRequest = new SetQueueAttributesRequest().withQueueUrl(standardQueueUrl)
                .addAttributesEntry("RedrivePolicy", "{\"maxReceiveCount\":\"2\", " + "\"deadLetterTargetArn\":\"" + deadLetterQueueARN + "\"}");

        sqs.setQueueAttributes(queueAttributesRequest);

        // Send a message to a standard queue

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        messageAttributes.put("AttributeOne", new MessageAttributeValue().withStringValue("This is an attribute")
                .withDataType("String"));

        SendMessageRequest sendMessageStandardQueue = new SendMessageRequest().withQueueUrl(standardQueueUrl)
                .withMessageBody("A simple message.- javasdkdemo-queue")
                .withDelaySeconds(30) // Message will arrive in the queue after 30 seconds. We can use this only in standard queues
                .withMessageAttributes(messageAttributes);

        sqs.sendMessage(sendMessageStandardQueue);

        // Send a message to a fifo queue

        SendMessageRequest sendMessageFifoQueue = new SendMessageRequest().withQueueUrl(fifoQueueUrl)
                .withMessageBody("FIFO Queue")
                .withMessageGroupId("javasdkdemo-group-1")
                .withMessageAttributes(messageAttributes);

        sqs.sendMessage(sendMessageFifoQueue);

        // Send multiple messages

        List<SendMessageBatchRequestEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new SendMessageBatchRequestEntry().withId("id-1")
                .withMessageBody("batch-1")
                .withMessageGroupId("javasdkdemo-group-1"));
        messageEntries.add(new SendMessageBatchRequestEntry().withId("id-2")
                .withMessageBody("batch-2")
                .withMessageGroupId("javasdkdemo-group-1"));

        SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(fifoQueueUrl, messageEntries);
        sqs.sendMessageBatch(sendMessageBatchRequest);

        // Read a message from a queue

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fifoQueueUrl).withWaitTimeSeconds(10) // Long polling;
                .withMaxNumberOfMessages(1); // Max is 10

        List<Message> sqsMessages = sqs.receiveMessage(receiveMessageRequest)
                .getMessages();

        sqsMessages.get(0)
                .getAttributes();
        sqsMessages.get(0)
                .getBody();

        // Delete a message from a queue

        sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(fifoQueueUrl)
                .withReceiptHandle(sqsMessages.get(0)
                        .getReceiptHandle()));

        // Monitoring
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(standardQueueUrl).withAttributeNames("All");
        GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(getQueueAttributesRequest);
        System.out.println(String.format("The number of messages on the queue: %s", getQueueAttributesResult.getAttributes()
                .get("ApproximateNumberOfMessages")));
        System.out.println(String.format("The number of messages in flight: %s", getQueueAttributesResult.getAttributes()
                .get("ApproximateNumberOfMessagesNotVisible")));

    }

}
