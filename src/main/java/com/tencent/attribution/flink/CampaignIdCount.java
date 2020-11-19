package com.tencent.attribution.flink;

import com.tencent.attribution.flink.config.Configs;
import com.tencent.attribution.flink.connectors.TubeSourceFunction;
import com.tencent.attribution.flink.flatmapper.CampaignIdCountFlatMapper;
import com.tencent.attribution.flink.functions.BoundedOutOfOrdernessGenerator;
import com.tencent.attribution.flink.serialization.DeserializationSchema;
import com.tencent.attribution.flink.serialization.ProtobufDeserializationSchema;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.tube.TubeOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;

import static com.tencent.attribution.flink.proto.gdt.GDTHttpPingService.HttpPingRecord;

/**
 * @author v_zefanliu
 */
public class CampaignIdCount {
    private static final Logger LOG = LoggerFactory.getLogger(CampaignIdCount.class);
    private static final String SESSION_KEY_NAME = "exposure_data";
    private static final int MSG_PER_SECOND_RATE_LIMIT = 100;
    private static final String FLINK_JOB_NAME = "flink count";
    private static final int STREAM_PARALLELISM_NUM = 90;

    public static void main(String[] args) throws Exception {
        // set up redis configuration
//        RedisConfigs redisConfigs = new Yaml().loadAs(getResourceAsStream("redis.yaml"), RedisConfigs.class);
//        LOG.info("redis configs are set up");

        // create a redis client
//        RedisStorage redisStorage = new RedisStorage(redisConfigs);

        // set up global configuration for the Flink job
        Configuration globalConfigs = new Configuration();
        globalConfigs.setString("JOB_NAME", FLINK_JOB_NAME);

        // Set up session configuration for the Flink job
        Configuration sessionConfigs = new Configuration();
        sessionConfigs.setString(TubeOptions.SESSION_KEY, SESSION_KEY_NAME);
        sessionConfigs.setBoolean(TubeOptions.BOOTSTRAP_FROM_MAX, true);
        LOG.info("Session configs are set up.");

        // set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(globalConfigs);
        LOG.info("Flink execution environment is set up");

        // Register serialization types
        env.getConfig().registerTypeWithKryoSerializer(HttpPingRecord.class, JavaSerializer.class);
        LOG.info("Protobuf serialization types are registered.");

        // Specify deserialization schema for data from the upstream
        // use class GDTHttpPingService.HttpPingRecord to serialize data
        // 这个 protobufDeserializationSchema 类用于反序列化
        ProtobufDeserializationSchema<HttpPingRecord> protobufDeserializationSchema = new ProtobufDeserializationSchema<>(HttpPingRecord.class);

        // get data source from tubeMQ
        TubeSourceFunction<HttpPingRecord> exposureDataSource = getSourceFromTubeMQ(sessionConfigs, protobufDeserializationSchema, MSG_PER_SECOND_RATE_LIMIT);

        // Raw data layer for exposure data
        DataStream<HttpPingRecord> dataStream = env
                .addSource(exposureDataSource)
                .returns(HttpPingRecord.class)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        //
        LOG.info("start to collect and process data");
        DataStream<Tuple2<String, Integer>> resultStream = dataStream
                .flatMap(new CampaignIdCountFlatMapper())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 将序号为1的字段求和
                .sum(1);

        // fixme
        // storage the result into redis
//        resultStream.map(new MapFunction<Tuple2<String, Integer>, Object>() {
//            @Override
//            public Object map(Tuple2<String, Integer> value) throws Exception {
//                redisStorage.storage(value.f0, value.f1.toString());
//                return null;
//            }
//        });
        resultStream.print();

        // execute flink job
        env.execute(FLINK_JOB_NAME);
    }

    /**
     * 该方法用于从TubeMQ获取到数据源
     *
     * @param sessionConfigs
     * @param deserializationSchema
     * @param msgPerSecondRateLimit
     * @return
     */
    private static TubeSourceFunction<HttpPingRecord> getSourceFromTubeMQ(
            Configuration sessionConfigs,
            DeserializationSchema<HttpPingRecord> deserializationSchema,
            int msgPerSecondRateLimit) {

        return new TubeSourceFunction<>(
                Configs.MASTER_ADDRESS,
                Configs.TOPIC_NAME,
                new TreeSet<>(),
                Configs.CONSUMER_GROUP_NAME,
                sessionConfigs,
                deserializationSchema,
                new GuavaFlinkConnectorRateLimiter(),
                msgPerSecondRateLimit
        );
    }
}
