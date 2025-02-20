package sbp.school.kafka.scheduler.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.AckDto;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.services.BaseProducerService;
import sbp.school.kafka.store.ConsumerTransactionStore;
import sbp.school.kafka.utils.HashUtils;

import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

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
            var ack = prepareAck();
            if (ack.isPresent()) {
                ackProducerService.send(ack.get().getHash(), ack.get());
            }
        } catch (Exception e) {
            logger.error("Ошибка при попытке отправки сообщений в топик обратного потока", e);
        }
    }

    /**
     * Сформировать подтверждение
     *
     * @return подтверждение
     * @throws Exception исключение
     */
    private Optional<AckDto> prepareAck() throws Exception {
        Long currentTimeStamp = Calendar.getInstance().getTimeInMillis();

        List<TransactionDto> transactions = ConsumerTransactionStore.STORE.stream()
                .filter(item -> currentTimeStamp - item.getTimestamp() >= timeWindow)
                .toList();

        if (transactions.isEmpty()) {
            return Optional.empty();
        }

        ConsumerTransactionStore.STORE.removeAll(transactions);

        // находим верхнюю и нижнюю границу временого отрезка
        var bottomTimeBorderTransaction = transactions.stream()
                .min(Comparator.comparing(TransactionDto::getTimestamp))
                .orElse(new TransactionDto());

        var upperTimeBorderTransaction = transactions.stream()
                .max(Comparator.comparing(TransactionDto::getTimestamp))
                .orElse(new TransactionDto());

        return Optional.of(new AckDto(bottomTimeBorderTransaction.getTimestamp(),
                                upperTimeBorderTransaction.getTimestamp(),
                                HashUtils.calcHashSum(transactions
                                    .stream()
                                    .map(item -> item.getTransaction().getUuid())
                                    .toList())));
    }
}
