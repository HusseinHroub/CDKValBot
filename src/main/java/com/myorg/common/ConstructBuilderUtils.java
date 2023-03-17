package com.myorg.common;


import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.DeduplicationScope;
import software.amazon.awscdk.services.sqs.FifoThroughputLimit;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

public class ConstructBuilderUtils {


    public static Queue getQueueWithDefaultSettingsAndDLQ(final Construct scope, final String queueName) {
        final String dlqName = queueName.endsWith(".fifo") ? queueName.split("\\.fifo")[0] + "DLQ.fifo" : queueName + "DLQ";
        return getDefaultQueueBuilder(scope, queueName)
                .deadLetterQueue(getDLQWithDefaultSettings(scope, dlqName))
                .build();
    }

    public static Queue.Builder getDefaultQueueBuilder(final Construct scope, final String queueName) {
        Queue.Builder queueBuilder = Queue.Builder.create(scope, queueName)
                .receiveMessageWaitTime(Duration.seconds(20))
                .queueName(queueName)
                .visibilityTimeout(Duration.minutes(3))
                .retentionPeriod(Duration.days(14));
        if (queueName.endsWith(".fifo")) {
            queueBuilder.fifo(true)
                    .deduplicationScope(DeduplicationScope.MESSAGE_GROUP)
                    .fifoThroughputLimit(FifoThroughputLimit.PER_MESSAGE_GROUP_ID);
        }

        return queueBuilder;

    }

    public static DeadLetterQueue getDLQWithDefaultSettings(final Construct scope, final String deadLetterQueueName) {
        return DeadLetterQueue.builder().queue(getDefaultQueueBuilder(scope, deadLetterQueueName).build()).maxReceiveCount(3).build();
    }

}
