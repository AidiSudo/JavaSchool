package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entities.AckDto;
import sbp.school.kafka.entities.OperationType;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ProducerTransactionStore;
import sbp.school.kafka.utils.HashUtils;

import java.util.Arrays;
import java.util.Collections;

/**
 * Тесты {@link AckConsumerServiceImpl}
 */
class AckConsumerServiceImplTest extends BaseConsumerServiceTest{
    private static final TransactionDto FIRST_TRANSACTION = new TransactionDto
            (new Transaction(OperationType.DEBIT, 200L, "234"), 120L);
    private static final TransactionDto SECOND_TRANSACTION = new TransactionDto
            (new Transaction(OperationType.ARREST, 300L, "123"), 200L);

    private Throwable pollException;

    /**
     * Успешный сценарий при равенстве хэшей
     * @throws Exception исключение
     */
    @Test
    public void testStartListenSuccessHashEquals() throws Exception {
        var ack = new AckDto(100L, 200L, calcHash());

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(getTopicName(), PARTITION)));
            consumer.addRecord(new ConsumerRecord(getTopicName(), PARTITION, 0L,
                    ack.getHash(), ack));
        });
        consumer.schedulePollTask(() -> consumerService.stop());

        setOffset();

        ProducerTransactionStore.SENDING_TRANSACTIONS.add(FIRST_TRANSACTION);
        ProducerTransactionStore.SENDING_TRANSACTIONS.add(SECOND_TRANSACTION);

        consumerService.startListen();

        Assertions.assertTrue(ProducerTransactionStore.TRANSACTIONS_FOR_SEND.isEmpty());
        Assertions.assertTrue(consumer.closed());
    }

    /**
     * Успешный сценарий при неравенстве хэшей
     * @throws Exception исключение
     */
    @Test
    public void testStartListenSuccessHashNotEquals() throws Exception {
        var ack = new AckDto(100L, 200L, "MD5");

        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(getTopicName(), PARTITION)));
            consumer.addRecord(new ConsumerRecord(getTopicName(), PARTITION, 0L,
                    ack.getHash(), ack));
        });
        consumer.schedulePollTask(() -> consumerService.stop());

        setOffset();

        ProducerTransactionStore.SENDING_TRANSACTIONS.add(FIRST_TRANSACTION);
        ProducerTransactionStore.SENDING_TRANSACTIONS.add(SECOND_TRANSACTION);

        consumerService.startListen();

        Assertions.assertTrue(ProducerTransactionStore.TRANSACTIONS_FOR_SEND.contains(FIRST_TRANSACTION));
        Assertions.assertTrue(ProducerTransactionStore.TRANSACTIONS_FOR_SEND.contains(SECOND_TRANSACTION));
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
        return new AckConsumerServiceImpl(consumer, (e) -> pollException = e);
    }

    @Override
    protected String getTopicName() {
        return "transaction_topic";
    }

    private String calcHash() throws Exception {
        return HashUtils.calcHashSum(Arrays.stream(new TransactionDto[]
                {FIRST_TRANSACTION, SECOND_TRANSACTION})
                .map(item -> item.getTransaction().getUuid())
                .toList());
    }
}