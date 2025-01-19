package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ConsumerTransactionStore;

import java.util.Properties;

/**
 * Слушатель транзакций
 */
public class TransactionConsumerService extends BaseConsumerService<TransactionDto> {
    /**
     * ctor
     *
     * @param properties проперти
     */
    public TransactionConsumerService(Properties properties) {
        super(properties);
    }

    protected void processRecord(ConsumerRecords<String, TransactionDto> records) {
        for (var record : records) {
            logger.trace(String.format("Получена и обработана запись по транзакции c ключом %s из партиции %d из топика %s",
                    record.value().getTransaction().getUuid(), record.partition(), record.topic()));

            ConsumerTransactionStore.STORE.add(record.value());
        }
    }
}
