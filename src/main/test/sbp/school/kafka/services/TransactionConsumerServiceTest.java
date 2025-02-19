package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entities.OperationType;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ConsumerTransactionStore;

import java.util.Collections;

/**
 * Тесты {@link TransactionConsumerService}
 */
public class TransactionConsumerServiceTest extends BaseConsumerServiceTest {
    private static final TransactionDto TRANSACTION = new TransactionDto(
            new Transaction(OperationType.DEBIT, 123, "123"), 123L);

    private Throwable pollException;

    /**
     * Сценарии с упешной обработкой записи
     */
    @Test
    public void testStartListenSuccess() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(getTopicName(), PARTITION)));
            consumer.addRecord(new ConsumerRecord(getTopicName(), PARTITION, 0L,
                    TRANSACTION.getTransaction().getUuid(), TRANSACTION));
        });
        consumer.schedulePollTask(() -> consumerService.stop());

        setOffset();

        Assertions.assertEquals(0, ConsumerTransactionStore.STORE.size());

        consumerService.startListen();

        Assertions.assertEquals(1, ConsumerTransactionStore.STORE.size());

        Assertions.assertTrue(consumer.closed());
    }

    /**
     * Сценарий с обработкой ошибки
     */
    @Test
    public void testStartListenFail() {
        String errorMessage = "Something went wrong";

        consumer.schedulePollTask(() -> consumer.setPollException(new KafkaException(errorMessage)));

        consumer.schedulePollTask(() -> consumerService.stop());

        setOffset();

        consumerService.startListen();

        Assertions.assertTrue(pollException instanceof KafkaException);
        Assertions.assertEquals(errorMessage, pollException.getMessage());
        Assertions.assertTrue(consumer.closed());
    }

    @Override
    protected BaseConsumerService getConsumerService() {
        return new TransactionConsumerService(consumer, (e) -> pollException = e);
    }

    @Override
    protected String getTopicName() {
        return "transaction_topic";
    }
}
