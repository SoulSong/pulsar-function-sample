package com.shf.pulsar.function.route;

import com.shf.pulsar.function.LevelInfo;
import com.shf.pulsar.function.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.shf.pulsar.function.route.Constant.ROUTE_LEVEL1_TOPIC;
import static com.shf.pulsar.function.route.Constant.ROUTE_LEVEL2_TOPIC;
import static com.shf.pulsar.function.route.Constant.ROUTE_ORIGIN_TOPIC;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/18 0:10
 */
@Slf4j
public class RoutingProducer {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientFactory.createPulsarClient();
        Producer<LevelInfo> producer = pulsarClient
                .newProducer(Schema.JSON(LevelInfo.class))
                .topic(ROUTE_ORIGIN_TOPIC)
                .create();

        log.info("msgId : {}", producer.newMessage().value(LevelInfo.builder().index(1).level("level2").build()).send().toString());
        log.info("msgId : {}", producer.newMessage().value(LevelInfo.builder().index(2).level("level1").build()).send().toString());
        log.info("msgId : {}", producer.newMessage().value(LevelInfo.builder().index(3).level("level1").build()).send().toString());
        log.info("msgId : {}", producer.newMessage().value(LevelInfo.builder().index(4).level("level1").build()).send().toString());
        log.info("msgId : {}", producer.newMessage().value(LevelInfo.builder().index(5).level("level2").build()).send().toString());

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        String subscriptionname = "my-subscription";
        Consumer<LevelInfo> level1Consumer = pulsarClient.newConsumer(Schema.JSON(LevelInfo.class))
                .topic(ROUTE_LEVEL1_TOPIC)
                .consumerName("level1-consumer")
                .subscriptionName(subscriptionname)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Consumer<LevelInfo> level2Consumer = pulsarClient.newConsumer(Schema.JSON(LevelInfo.class))
                .topic(ROUTE_LEVEL2_TOPIC)
                .consumerName("level2-consumer")
                .subscriptionName(subscriptionname)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        executorService.submit(() -> {
            try {
                consumer(level1Consumer);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });

        executorService.submit(() -> {
            try {
                consumer(level2Consumer);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
    }

    public static void consumer(Consumer<LevelInfo> consumer) throws PulsarClientException {
        while (true) {
            Messages<LevelInfo> messages = consumer.batchReceive();
            messages.forEach(message -> {
                try {
                    log.info("consumer: {}; msgId: {}; value: {}; topic: {}", consumer.getConsumerName(), message.getMessageId().toString(),
                            message.getValue().toString(), message.getTopicName());
                    consumer.acknowledge(message);
                } catch (Exception e) {
                    log.error("invoke error , message : {}", e.getMessage());
                    consumer.negativeAcknowledge(message);
                }
            });
        }
    }
}
