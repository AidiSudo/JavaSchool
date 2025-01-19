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

        Map<Long, List<TransactionDto>> aggregatedTransactionsByTimeStamp = new HashMap<>();

        for (var transcation : ProducerTransactionStore.SENDING_TRANSACTIONS) {
            List<TransactionDto> transactionDtos = aggregatedTransactionsByTimeStamp.get(transcation.getTimestamp());

            if (transactionDtos == null) {
                transactionDtos = new ArrayList<>();

                aggregatedTransactionsByTimeStamp.put(transcation.getTimestamp(), transactionDtos);
            }

            transactionDtos.add(transcation);
        }

        for (var record : records) {
            var ack = record.value();

            List<TransactionDto> transactions = aggregatedTransactionsByTimeStamp.get(ack.getTimestamp());

            if (transactions != null && !transactions.isEmpty()) {
                try {
                    String hashSum = HashUtils.calcHashSum(transactions.stream()
                            .map(item -> item.getTransaction().getUuid())
                            .collect(Collectors.toList()));

                    if (!hashSum.equals(ack.getHash())) {
                        logger.info(String.format("Хэши не совпали, транзакции с timestamp %d буду отправлены повторно",
                                transactions.get(0).getTimestamp()));

                        ProducerTransactionStore.TRANSACTIONS_FOR_SEND.addAll(transactions);
                    } else {
                        logger.info(String.format("Хэши для транзакций с timestamp %d совпали",
                                transactions.get(0).getTimestamp()));
                    }

                    ProducerTransactionStore.SENDING_TRANSACTIONS.removeAll(transactions);

                } catch (Exception e) {
                    logger.error(String.format("Ошибка подсчета хэш суммы для записей с временной меткой %d",
                            transactions.get(0).getTimestamp()));
                }
            }
        }
    }
}
