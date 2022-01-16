package io.github.devlibx.easy.flink.functions.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * A generic container class to hold no of events received.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCount implements Serializable {
    private int count;

    public String toString() {
        Date start = new Date(System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss:S");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("IST"));
        return String.format("%d %s", count, simpleDateFormat.format(start));
    }
}
