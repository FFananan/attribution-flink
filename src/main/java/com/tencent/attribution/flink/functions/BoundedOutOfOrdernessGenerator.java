package com.tencent.attribution.flink.functions;

import com.tencent.attribution.flink.proto.gdt.GDTHttpPingService;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<GDTHttpPingService.HttpPingRecord> {

    private static Logger LOG = LoggerFactory.getLogger(BoundedOutOfOrdernessGenerator.class);

    private static final long MAX_OUT_OF_ORDERNESS = 10000;

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long timestamp = currentMaxTimestamp - MAX_OUT_OF_ORDERNESS;
        return new Watermark(timestamp);
    }

    @Override
    public long extractTimestamp(GDTHttpPingService.HttpPingRecord element, long recordTimestamp) {
        long timestamp = element.getProcessTime() * 1000L;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
