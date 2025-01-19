package sbp.school.kafka.store;

import sbp.school.kafka.entities.TransactionDto;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Хранилище транзакций, которое хранит обработанные консьюмером транзакций записи
 */
public class ConsumerTransactionStore {
    public static BlockingQueue<TransactionDto> STORE = new LinkedBlockingQueue<>();
}
