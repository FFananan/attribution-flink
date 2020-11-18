package com.tencent.attribution.flink.connectors;


import com.tencent.attribution.flink.serialization.DeserializationSchema;
import com.tencent.tdbank.msg.TDMsg1;
import com.tencent.tubemq.client.config.ConsumerConfig;
import com.tencent.tubemq.client.consumer.ConsumerResult;
import com.tencent.tubemq.client.consumer.PullMessageConsumer;
import com.tencent.tubemq.client.factory.TubeSingleSessionFactory;
import com.tencent.tubemq.corebase.Message;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.tube.TubeOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.runtime.net.ConnectionUtils.findConnectingAddress;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.TimeUtils.parseDuration;

/**
 * The Flink Tube Consumer.
 *
 */
public class TubeSourceFunction<T>
        extends RichParallelSourceFunction<T> implements CheckpointedFunction {

    private static final Logger LOG =
            LoggerFactory.getLogger(TubeSourceFunction.class);

    private static final String TUBE_OFFSET_STATE = "tube-offset-state";

    private static final String SPLIT_COMMA = ",";
    private static final String SPLIT_COLON = ":";

    /**
     * The address of hippo master, format eg: 127.0.0.1:8080,127.0.0.2:8081.
     */
    private final String masterAddress;

    /**
     * The topic name.
     */
    private final String topic;

    /**
     * The tube consumers use this tid set to filter records reading from server.
     */
    private final TreeSet<String> tidSet;

    /**
     * The consumer group name.
     */
    private final String consumerGroup;

    /**
     * The random key for tube consumer group when startup.
     */
    private final String sessionKey;

    /**
     * True if consuming message from max offset.
     */
    private final boolean consumeFromMax;

    /**
     * The time to wait if tube broker returns message not found.
     */
    private final Duration messageNotFoundWaitPeriod;

    /**
     * The max time to marked source idle.
     */
    private final Duration maxIdleTime;

    /**
     * Flag indicating whether the consumer is still running.
     **/
    private volatile boolean running;

    /**
     * The state for the offsets of queues.
     * 这里transient关键字的作用是：在不需要被序列化的字段前添加这个关键字，那么这个字段就不会被序列化
     */
    private transient ListState<Tuple2<String, Long>> offsetsState;

    /**
     * The current offsets of partitions which are stored in {@link #offsetsState}
     * once a checkpoint is triggered.
     *
     * <p>NOTE: The offsets are populated in the main thread and saved in the
     * checkpoint thread. Its usage must be guarded by the checkpoint lock.</p>
     */
    private transient Map<String, Long> currentOffsets;

    /**
     * The tube session factory.
     */
    private transient TubeSingleSessionFactory messageSessionFactory;

    /**
     * The tube pull consumer.
     */
    private transient PullMessageConsumer messagePullConsumer;

    private DeserializationSchema<T> deserializationSchema;
    private final FlinkConnectorRateLimiter rateLimiter;
    private final int messagePerSecondRateLimit;

    private transient Counter input_qps;

    public TubeSourceFunction(
            String masterAddress,
            String topic,
            TreeSet<String> tidSet,
            String consumerGroup,
            Configuration configuration,
            DeserializationSchema<T> deserializationSchema,
            FlinkConnectorRateLimiter rateLimiter,
            int messagePerSecondRateLimit
    ) {
        checkNotNull(masterAddress,
                "The master address must not be null.");
        checkNotNull(topic,
                "The topic must not be null.");
        checkNotNull(tidSet,
                "The tid set must not be null.");
        checkNotNull(consumerGroup,
                "The consumer group must not be null.");
        checkNotNull(configuration,
                "The configuration must not be null.");
        checkNotNull(deserializationSchema,
                "The deserialization schema must not be null.");

        this.masterAddress = masterAddress;
        this.topic = topic;
        this.tidSet = tidSet;
        this.consumerGroup = consumerGroup;

        this.sessionKey =
                configuration.getString(TubeOptions.SESSION_KEY);
        this.consumeFromMax =
                configuration.getBoolean(TubeOptions.BOOTSTRAP_FROM_MAX);
        this.messageNotFoundWaitPeriod =
                parseDuration(
                        configuration.getString(
                                TubeOptions.MESSAGE_NOT_FOUND_WAIT_PERIOD));
        this.maxIdleTime =
                parseDuration(
                        configuration.getString(
                                TubeOptions.SOURCE_MAX_IDLE_TIME));

        this.deserializationSchema = deserializationSchema;
        this.rateLimiter = rateLimiter;
        this.messagePerSecondRateLimit = messagePerSecondRateLimit;
    }

    @Override
    public void initializeState(
            FunctionInitializationContext context
    ) throws Exception {

        TypeInformation<Tuple2<String, Long>> typeInformation =
                new TupleTypeInfo<>(STRING_TYPE_INFO, LONG_TYPE_INFO);
        ListStateDescriptor<Tuple2<String, Long>> stateDescriptor =
                new ListStateDescriptor<>(TUBE_OFFSET_STATE, typeInformation);

        OperatorStateStore stateStore = context.getOperatorStateStore();
        offsetsState = stateStore.getListState(stateDescriptor);

        currentOffsets = new HashMap<>();
        if (context.isRestored()) {
            for (Tuple2<String, Long> tubeOffset : offsetsState.get()) {
                currentOffsets.put(tubeOffset.f0, tubeOffset.f1);
            }

            LOG.info("Successfully restore the offsets {}.", currentOffsets);
        } else {
            LOG.info("No restore offsets.");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        LOG.info("Opening the tube source (masterAddress: {}, topic: {}, " +
                        "consumerGroup: {}, tidSet: {}).",
                masterAddress, topic, consumerGroup, tidSet);

        String firstAddress = masterAddress.split(SPLIT_COMMA)[0];
        String[] firstAddressSegments = firstAddress.split(SPLIT_COLON);
        String firstHost = firstAddressSegments[0];
        int firstPort = Integer.parseInt(firstAddressSegments[1]);
        InetSocketAddress firstSocketAddress =
                new InetSocketAddress(firstHost, firstPort);

        InetAddress localAddress =
                findConnectingAddress(firstSocketAddress, 2000, 400);
        String localhost = localAddress.getHostAddress();

        ConsumerConfig consumerConfig =
                new ConsumerConfig(localhost, masterAddress, consumerGroup);
        consumerConfig
                .setConsumeModel(consumeFromMax ? 1 : 0);
        consumerConfig
                .setMsgNotFoundWaitPeriodMs(messageNotFoundWaitPeriod.toMillis());

        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        deserializationSchema.initialize(parameters);

        rateLimiter.setRate(messagePerSecondRateLimit * getRuntimeContext().getNumberOfParallelSubtasks());
        rateLimiter.open(getRuntimeContext());
        messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);

        while (true) {
            try {
                messagePullConsumer =
                        messageSessionFactory.createPullConsumer(consumerConfig);
                messagePullConsumer
                        .subscribe(topic, tidSet);
                messagePullConsumer
                        .completeSubscribe(sessionKey, numTasks, true, currentOffsets);

                running = true;
                break;
            } catch (Exception e) {
                LOG.error("oceanus#trace# error:" + e.getMessage(), e);
                if (this.messagePullConsumer != null) {
                    try {
                        this.messagePullConsumer.shutdown();
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
                LOG.warn("oceanus#trace# -----------------in open() exception sleep for 1s.");
                Thread.sleep(1000L);
            }
        }

    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {

        Instant lastConsumeInstant = Instant.now();

        while (running) {
            Thread.sleep(2500);
            ConsumerResult consumeResult = messagePullConsumer.getMessage();
            if (!consumeResult.isSuccess()) {
                if (consumeResult.getErrMsg().contains("All partition in waiting")) {
                    Thread.sleep(400);
                    continue;
                }
                //LOG.info("Could not consume messages from tube (errcode: {}, " +
                                //"errmsg: {}).", consumeResult.getErrCode(),
                        //consumeResult.getErrMsg());

                Duration idleTime =
                        Duration.between(lastConsumeInstant, Instant.now());
                if (idleTime.compareTo(maxIdleTime) > 0) {
                    LOG.info("Mark this source as temporarily idle.");
                    ctx.markAsTemporarilyIdle();

                }

                continue;
            }

            List<Message> messageList = consumeResult.getMessageList();
            //LOG.info("message size is {}", messageList.size());
            int count = 0;
            synchronized (ctx.getCheckpointLock()) {
                for (Message message : messageList) {
                    TDMsg1 tdMsg1 = TDMsg1.parseFrom(message.getData());
                    for (String attr: tdMsg1.getAttrs()) {
                        Iterator<byte[]> it = tdMsg1.getIterator(attr);
                        if (it != null) {
                            while (it.hasNext()) {
                                byte[] body = it.next();
                                if (body != null && body.length > 0) {
                                    T record = deserializationSchema.deserialize(body);
                                    count++;
                                    try {
                                        // todo
                                        ctx.collect(record);
                                        input_qps.inc();
                                    } catch (Exception e) {
                                        LOG.error(ExceptionUtils.getStackTrace(e));
                                    }
                                }
                            }
                        }
                    }
                }

                currentOffsets.put(
                        consumeResult.getPartitionKey(),
                        consumeResult.getCurrOffset()
                );
            }
            rateLimiter.acquire(count);

            ConsumerResult confirmResult =
                    messagePullConsumer
                            .confirmConsume(consumeResult.getConfirmContext(), true);
            if (!confirmResult.isSuccess()) {
                LOG.warn("Could not confirm messages to tube (errcode: {}, " +
                                "errmsg: {}).", confirmResult.getErrCode(),
                        confirmResult.getErrMsg());
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        if (!running) {
            LOG.info("snapshotState() called on closed source; returning null.");
        } else {
            offsetsState.clear();
            for (Map.Entry<String, Long> entry : currentOffsets.entrySet()) {
                offsetsState.add(new Tuple2<>(entry.getKey(), entry.getValue()));
            }

            LOG.info("Successfully save the offsets in checkpoint {}: {}.",
                    context.getCheckpointId(), currentOffsets);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {

        cancel();

        if (messagePullConsumer != null) {
            try {
                messagePullConsumer.shutdown();
            } catch (Throwable t) {
                LOG.warn("Could not properly shutdown the tube pull consumer.",
                        t);
            }
        }

        if (messageSessionFactory != null) {
            try {
                messageSessionFactory.shutdown();
            } catch (Throwable t) {
                LOG.warn("Could not properly shutdown the tube session " +
                        "factory.", t);
            }
        }

        super.close();


        LOG.info("Closed the tube source.");
    }
}
