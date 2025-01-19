package sbp.school.kafka.store;

import sbp.school.kafka.entities.TransactionDto;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Хранилище транзакций, которое хранит отправленные записи и записи для отправки
 */
public class ProducerTransactionStore {
    public static BlockingQueue<TransactionDto> SENDING_TRANSACTIONS = new LinkedBlockingQueue<>();
    public static BlockingQueue<TransactionDto> TRANSACTIONS_FOR_SEND = new LinkedBlockingQueue<>();
}