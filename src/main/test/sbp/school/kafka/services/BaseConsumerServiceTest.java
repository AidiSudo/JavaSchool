package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Тесты для {@link BaseConsumerService}
 */
abstract class BaseConsumerServiceTest {
    protected static final int PARTITION = 0;

    protected MockConsumer consumer;
    protected List<ConsumerRecord> records;
    protected BaseConsumerService consumerService;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
        records = new ArrayList<>();
        consumerService = getConsumerService();
    }

    protected void setOffset() {
        Map<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(getTopicName(), PARTITION);
        startingOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startingOffsets);
    }

    protected abstract BaseConsumerService getConsumerService();
    protected abstract String getTopicName();
}