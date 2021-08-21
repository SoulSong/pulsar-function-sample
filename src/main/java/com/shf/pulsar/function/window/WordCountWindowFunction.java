package com.shf.pulsar.function.window;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/19 0:27
 */
public class WordCountWindowFunction implements WindowFunction<String, Void> {
    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        Logger logger = context.getLogger();
        logger.info("current window receive {} records.", inputs.size());
        for (Record<String> input : inputs) {
            Arrays.asList(input.getValue().split(" ")).forEach(word -> context.incrCounter(word, 1));
        }
        return null;
    }
}
