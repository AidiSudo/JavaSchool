package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ConsumerTransactionStore;
import sbp.school.kafka.utils.Constants;

/**
 * Слушатель транзакций
 */
public class TransactionConsumerService extends BaseConsumerService<TransactionDto> {
    /**
     * ctor
     *
     * @param consumer потребитель
     */
    public TransactionConsumerService(Consumer consumer) {
        super(consumer, (e) -> logger.error("Ошибка обработки сообщений из брокера"));
    }

    /**
     * ctor
     *
     * @param consumer потребитель
     * @param exceptionConsumer call-back в случае исключения
     */
    public TransactionConsumerService(Consumer consumer, java.util.function.Consumer<Throwable> exceptionConsumer) {
        super(consumer, exceptionConsumer);
    }

    @Override
    protected String getTopicName() {
        return Constants.TRANSACTION_TOPIC;
    }

    protected void processRecord(ConsumerRecords<String, TransactionDto> records) {
        for (var record : records) {
            logger.trace(String.format("Получена и обработана запись по транзакции c ключом %s из партиции %d из топика %s",
                    record.value().getTransaction().getUuid(), record.partition(), record.topic()));

            ConsumerTransactionStore.STORE.add(record.value());
        }
    }
}
