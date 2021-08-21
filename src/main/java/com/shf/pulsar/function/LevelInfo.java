package com.shf.pulsar.function;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/18 10:25
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class LevelInfo {
    private String level;
    private int index;
}
