package sbp.school.kafka.scheduler.tasks;

import sbp.school.kafka.services.BaseProducerService;
import sbp.school.kafka.store.ProducerTransactionStore;

/**
 * Таск отправки данных об транзакциях в брокер
 */
public class SendTransactionsTask implements Runnable {
    private BaseProducerService producerService;

    /**
     * ctor
     *
     * @param producerService сервис отправки сообщений
     */
    public SendTransactionsTask(BaseProducerService producerService) {
        this.producerService = producerService;
    }

    @Override
    public void run() {
        for (var transcation : ProducerTransactionStore.TRANSACTIONS_FOR_SEND) {
            producerService.send(transcation.getTransaction().getUuid(),
                                 transcation);
        }
    }
}
