package sbp.school.kafka.services;

import org.apache.kafka.clients.producer.Producer;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ProducerTransactionStore;
import sbp.school.kafka.utils.Constants;

/**
 * Поставщик данных в кафку об транзакциях
 */
public class TransactionProducerServiceImpl extends BaseProducerService<TransactionDto> {
    /**
     * ctor
     *
     * @param producer поставщик
     */
    public TransactionProducerServiceImpl(Producer producer) {
        super(producer);
    }

    @Override
    protected String getTopicName() {
        return Constants.TRANSACTION_TOPIC;
    }

    @Override
    protected void handleSuccess(TransactionDto transaction) {
        ProducerTransactionStore.TRANSACTIONS_FOR_SEND.remove(transaction);
        ProducerTransactionStore.SENDING_TRANSACTIONS.add(transaction);
        
        logger.info(String.format("Запись c UUID %s успешно добавлена в брокер", transaction.getTransaction().getUuid()));
    }
}
