package sbp.school.kafka.scheduler.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.AckDto;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.services.BaseProducerService;
import sbp.school.kafka.store.ConsumerTransactionStore;
import sbp.school.kafka.utils.HashUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Таск шедуллера подсчета и отправки хешей идентификаторов транзакций
 */
public class BackFlowTopicTransactionTask implements Runnable {
    private final BaseProducerService ackProducerService;
    private final Long timeWindow;

    private static final Logger logger = LoggerFactory.getLogger(BackFlowTopicTransactionTask.class.getName());

    /**
     * ctor
     *
     * @param ackProducerService сервис отправки сообщений
     * @param timeWindow временной промежуток в мс,
     *                   по истечению которого будут отправлены транзакции в топик обратного потока
     */
    public BackFlowTopicTransactionTask(BaseProducerService ackProducerService, Long timeWindow) {
        this.ackProducerService = ackProducerService;
        this.timeWindow = timeWindow;
    }

    @Override
    public void run() {
        try {
            List<AckDto> acks = prepareAcks();

            for (var ack : acks) {
                ackProducerService.send(ack.getHash(), ack);
            }
        } catch (Exception e) {
            logger.error("Ошибка при попытке отправки сообщений в топик обратного потока", e);
        }
    }

    /**
     * Подготовить данные для отправки
     *
     * @return подтверждения
     * @throws Exception исключение
     */
    private List<AckDto> prepareAcks() throws Exception {
        Long currentTimeStamp = Calendar.getInstance().getTimeInMillis();

        List<TransactionDto> transactions = ConsumerTransactionStore.STORE.stream()
                .filter(item -> item.getTimestamp() + timeWindow < currentTimeStamp)
                .collect(Collectors.toList());

        if (transactions.isEmpty()) {
            return Collections.emptyList();
        }

        ConsumerTransactionStore.STORE.removeAll(transactions);

        Map<Long, List<TransactionDto>> aggregatedTransactionsByTimeStamp = new HashMap<>();

        for (var transcation : transactions) {
            List<TransactionDto> transactionDtos = aggregatedTransactionsByTimeStamp.get(transcation.getTimestamp());

            if (transactionDtos == null) {
                transactionDtos = new ArrayList<>();

                aggregatedTransactionsByTimeStamp.put(transcation.getTimestamp(), transactionDtos);
            }

            transactionDtos.add(transcation);
        }

        List<AckDto> acks = new ArrayList<>();

        for (var entry : aggregatedTransactionsByTimeStamp.entrySet()) {
            var transactionsGuids = entry.getValue().stream()
                    .map(item -> item.getTransaction().getUuid())
                    .collect(Collectors.toList());

            acks.add(new AckDto(entry.getKey(), HashUtils.calcHashSum(transactionsGuids)));
        }

        return acks;
    }
}
