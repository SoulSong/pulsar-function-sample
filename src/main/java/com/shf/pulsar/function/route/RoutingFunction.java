package com.shf.pulsar.function.route;

import com.shf.pulsar.function.LevelInfo;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;

import static com.shf.pulsar.function.route.Constant.ROUTE_LEVEL1_TOPIC;
import static com.shf.pulsar.function.route.Constant.ROUTE_LEVEL2_TOPIC;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/18 19:52
 */
public class RoutingFunction implements Function<LevelInfo, Void> {
    @Override
    public Void process(LevelInfo input, Context context) {
        Logger logger = context.getLogger();
        // 可通过record获取message原始信息以及schemaInfo
        Record<LevelInfo> record = (Record<LevelInfo>) context.getCurrentRecord();
        record.getMessage().ifPresent(message -> {
            logger.info("handle message [{}] with id [{}] from topic [{}], current env is {}", input.toString(), message.getMessageId().toString(),
                    String.join(", ", context.getInputTopics()), context.getUserConfigValueOrDefault("env", "unknown").toString());
            // https://pulsar.apache.org/docs/zh-CN/next/deploy-monitoring/#function-and-connector-stats
            context.recordMetric("MessageEventTime", message.getEventTime());
            try {
                if ("level1".equals(input.getLevel())) {
                    context.newOutputMessage(ROUTE_LEVEL1_TOPIC, Schema.JSON(LevelInfo.class)).value(input).send();
                } else {
                    context.newOutputMessage(ROUTE_LEVEL2_TOPIC, Schema.JSON(LevelInfo.class)).value(input).send();
                }
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        return null;
    }
}
