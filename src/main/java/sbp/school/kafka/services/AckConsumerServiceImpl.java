package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import sbp.school.kafka.entities.AckDto;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.store.ProducerTransactionStore;
import sbp.school.kafka.utils.HashUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Слушатель подтверждений
 */
public class AckConsumerServiceImpl extends BaseConsumerService<AckDto> {
    /**
     * ctor
     *
     * @param properties проперти
     */
    public AckConsumerServiceImpl(Properties properties) {
        super(properties);
    }

    @Override
    protected void processRecord(ConsumerRecords<String, AckDto> records) {
        if (records.isEmpty()) {
            return;
        }

        for (var record : records) {
            var ack = record.value();

            List<TransactionDto> transactions = ProducerTransactionStore.SENDING_TRANSACTIONS.stream()
                    .filter(transaction -> transaction.getTimestamp() >= ack.getStartTimeWindow() &&
                            transaction.getTimestamp() <= ack.getEndTimeWindow())
                    .toList();

            if (transactions != null && !transactions.isEmpty()) {
                try {
                    String hashSum = HashUtils.calcHashSum(transactions.stream()
                            .map(item -> item.getTransaction().getUuid())
                            .collect(Collectors.toList()));

                    if (!hashSum.equals(ack.getHash())) {
                        logger.info(String.format("Хэши не совпали, транзакции во временом промежутке %d - %d" +
                                        " буду отправлены повторно", ack.getStartTimeWindow(), ack.getEndTimeWindow()));

                        ProducerTransactionStore.TRANSACTIONS_FOR_SEND.addAll(transactions);
                    } else {
                        logger.info(String.format("Хэши для транзакций во временом промежутке %d - %d совпали",
                                ack.getStartTimeWindow(), ack.getEndTimeWindow()));
                    }

                    ProducerTransactionStore.SENDING_TRANSACTIONS.removeAll(transactions);

                } catch (Exception e) {
                    logger.error(String.format("Ошибка подсчета хэш суммы для записей во временом промежутке %d - %d",
                            ack.getStartTimeWindow(), ack.getEndTimeWindow()));
                }
            }
        }
    }
}
