package com.tencent.attribution.flink.flatmapper;

import com.tencent.attribution.flink.proto.gdt.GDTHttpPingService;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author v_zefanliu
 */
public class CampaignIdCountFlatMapper implements FlatMapFunction<GDTHttpPingService.HttpPingRecord, Tuple2<String, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(CampaignIdCountFlatMapper.class);

    @Override
    public void flatMap(GDTHttpPingService.HttpPingRecord value, Collector<Tuple2<String, Integer>> out) throws Exception {
        // TODO 在这里实现计数的逻辑
        // 这里传进来的类的类型可能需要替换
        // 大致思路是，将value中的campaignId作为key，然后数量为1作为一个Tuple2返回
        // 在外层处理就是将key进行keyBy操作，然后对value进行求和
        LOG.info("already received data and start to split string");
        String[] fields = value.getString5().split("###");
        // get the campaign ID
        String campaignId = fields[1];
        out.collect(Tuple2.of(campaignId, 1));
        LOG.info("collected tuple");
    }
}
