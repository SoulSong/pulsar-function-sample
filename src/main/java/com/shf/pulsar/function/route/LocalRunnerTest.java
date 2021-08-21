package com.shf.pulsar.function.route;

import com.google.common.collect.ImmutableMap;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;

import java.util.Collections;

import static com.shf.pulsar.function.PulsarClientFactory.SERVICE_URL;
import static com.shf.pulsar.function.route.Constant.ROUTE_ORIGIN_TOPIC;

/**
 * description :
 * 通过localRunner进行测试，其在本地启动一个线程来模拟function运行实例，其生产和消费的消息均面向真实pulsar cluster。
 *
 * @author songhaifeng
 * @date 2021/8/18 10:37
 */
public class LocalRunnerTest {
    public static void main(String[] args) throws Exception {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setName(RoutingFunction.class.getName());
        // 从input-topic获取数据
        functionConfig.setInputs(Collections.singleton(ROUTE_ORIGIN_TOPIC));
        functionConfig.setClassName(RoutingFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setUserConfig(ImmutableMap.of("env","qa"));
        LocalRunner localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
                .brokerServiceUrl(SERVICE_URL)
                .build();
        localRunner.start(true);
    }
}
