package com.shf.pulsar.function.window;

import com.shf.pulsar.function.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.FunctionState;

/**
 * description :
 * localRun模式下无法获取对应的state，仅有通过create部署至集群后，pulsarAdmin方可获取到function信息，如list，get，queryState等。
 *
 * @author songhaifeng
 * @date 2021/8/21 19:21
 */
@Slf4j
public class QueryState {
    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();
        FunctionState state = pulsarAdmin.functions().getFunctionState("public", "default", WordCountWindowFunction.class.getSimpleName(), "hello");
        if (state != null) {
            log.info("{}->{}", state.getKey(), state.getNumberValue());
        }
    }
}
