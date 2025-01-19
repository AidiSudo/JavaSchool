package sbp.school.kafka.services;

import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ProducerTransactionStore;

import java.util.Properties;

/**
 * Поставщик данных в кафку об транзакциях
 */
public class TransactionProducerServiceImpl extends BaseProducerService<TransactionDto> {
    /**
     * ctor
     *
     * @param properties настройки
     */
    public TransactionProducerServiceImpl(Properties properties) {
        super(properties);
    }

    @Override
    protected void handleSuccess(TransactionDto transaction) {
        ProducerTransactionStore.TRANSACTIONS_FOR_SEND.remove(transaction);
        ProducerTransactionStore.SENDING_TRANSACTIONS.add(transaction);
        
        logger.info(String.format("Запись c UUID %s успешно добавлена в брокер", transaction.getTransaction().getUuid()));
    }
}
