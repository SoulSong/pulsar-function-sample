package com.shf.pulsar.function.window;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.functions.LocalRunner;

import java.util.Collections;

import static com.shf.pulsar.function.PulsarClientFactory.SERVICE_URL;
import static com.shf.pulsar.function.PulsarClientFactory.STATE_STORAGE_SERVICE_URL;
import static com.shf.pulsar.function.window.Constant.WINDOW_TOPIC;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/20 23:35
 */
public class LocalRunnerTest {
    public static void main(String[] args) throws Exception {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setNamespace("default");
        functionConfig.setTenant("public");
        functionConfig.setName(WordCountWindowFunction.class.getSimpleName());
        functionConfig.setInputs(Collections.singleton(WINDOW_TOPIC));
        functionConfig.setClassName(WordCountWindowFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setWindowConfig(new WindowConfig().setWindowLengthCount(2));

        LocalRunner localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
                .brokerServiceUrl(SERVICE_URL)
                // 由于function中使用了state，故必须配置state存储url。
                .stateStorageServiceUrl(STATE_STORAGE_SERVICE_URL)
                .build();
        localRunner.start(true);
    }
}
