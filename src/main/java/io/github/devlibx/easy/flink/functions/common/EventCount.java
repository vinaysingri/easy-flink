package io.github.devlibx.easy.flink.functions.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * A generic container class to hold no of events received.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCount implements Serializable {
    private int count;
}
