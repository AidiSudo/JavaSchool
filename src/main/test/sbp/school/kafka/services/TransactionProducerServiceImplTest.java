package sbp.school.kafka.services;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entities.OperationType;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.partitioners.TransactionPartitioners;
import sbp.school.kafka.serializer.TransactionSerializer;
import sbp.school.kafka.store.ProducerTransactionStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Тесты для {@link TransactionProducerServiceImpl}
 */
class TransactionProducerServiceImplTest {
    private MockProducer<String, TransactionDto> producer;
    private TransactionProducerServiceImpl transactionProducerService;

    private static final TransactionDto TRANSACTION = new TransactionDto(
            new Transaction(OperationType.DEBIT, 123, "123"), 123L);

    @BeforeEach
    public void setUp() {
        this.producer = prepareProducerMock();
        this.transactionProducerService = new TransactionProducerServiceImpl(producer);
    }

    /**
     * Успешный кейс отправки
     */
    @Test
    public void testSendSuccess() {
        ProducerTransactionStore.TRANSACTIONS_FOR_SEND.add(TRANSACTION);

        transactionProducerService.send(TRANSACTION.getTransaction().getUuid(), TRANSACTION);

        TransactionDto transactionDto = producer.history().get(0).value();

        Assertions.assertEquals(transactionDto.getTimestamp(), TRANSACTION.getTimestamp());
        Assertions.assertEquals(transactionDto.getTransaction(), TRANSACTION.getTransaction());

        Assertions.assertFalse(ProducerTransactionStore.TRANSACTIONS_FOR_SEND.contains(TRANSACTION));
        Assertions.assertTrue(ProducerTransactionStore.SENDING_TRANSACTIONS.contains(TRANSACTION));

        producer.clear();
    }

    private MockProducer prepareProducerMock() {
        PartitionInfo partitionInfo0 = new PartitionInfo("transaction_topic", 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo("transaction_topic", 1, null, null, null);
        PartitionInfo partitionInfo2 = new PartitionInfo("transaction_topic", 2, null, null, null);

        List<PartitionInfo> list = new ArrayList<>();
        list.add(partitionInfo0);
        list.add(partitionInfo1);
        list.add(partitionInfo2);

        Cluster cluster = new Cluster("cluster", new ArrayList<>(), list,
                Collections.emptySet(),
                Collections.emptySet());

        return new MockProducer(cluster, true, new TransactionPartitioners(),
                new StringSerializer(), new TransactionSerializer());
    }
}