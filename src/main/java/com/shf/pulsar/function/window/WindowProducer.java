package com.shf.pulsar.function.window;

import com.shf.pulsar.function.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import static com.shf.pulsar.function.window.Constant.WINDOW_TOPIC;


/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/20 23:24
 */
@Slf4j
public class WindowProducer {

    public static void main(String[] args) throws PulsarClientException {

        PulsarClient pulsarClient = PulsarClientFactory.createPulsarClient();
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(WINDOW_TOPIC)
                .create();

        log.info("msgId : {}", producer.newMessage().value("hello foo").send().toString());
        log.info("msgId : {}", producer.newMessage().value("hello bar").send().toString());
        log.info("msgId : {}", producer.newMessage().value("hello car").send().toString());
        log.info("msgId : {}", producer.newMessage().value("hello bar").send().toString());
        log.info("msgId : {}", producer.newMessage().value("hello foo").send().toString());
        producer.close();
        pulsarClient.close();
    }
}
