package sbp.school.kafka.services;

import org.junit.jupiter.api.Test;
import sbp.school.kafka.entities.OperationType;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.entities.TransactionDto;
import sbp.school.kafka.scheduler.tasks.BackFlowTopicTransactionTask;
import sbp.school.kafka.scheduler.tasks.SendTransactionsTask;
import sbp.school.kafka.store.ProducerTransactionStore;
import sbp.school.kafka.utils.PropertyReader;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.*;

public class ProcessTest {
    @Test
    public void test() throws Exception {
        // Переменная для хранения промежутка времени в мс, после которого считается, что транзакции с единым timestamp
        // считаются доставленными
        final Long timeWindowMillis = 5000L;

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Properties propertiesForTransactionProducer = PropertyReader.readPropertiesFromFile("transactionTopicProducer.properties");
        Properties propertiesForTransactionConsumer = PropertyReader.readPropertiesFromFile("transactionTopicConsumer.properties");

        Properties propertiesForBackFlowProducer = PropertyReader.readPropertiesFromFile("backFlowTopicProducer.properties");
        Properties propertiesForBackFlowConsumer = PropertyReader.readPropertiesFromFile("backFlowTopicConsumer.properties");

        BaseProducerService producerServiceForTransaction = new TransactionProducerServiceImpl(propertiesForTransactionProducer);
        BaseProducerService producerServiceForAck = new AckProducerServiceImpl(propertiesForBackFlowProducer);

        // запуск наполнения хранилища транзакций
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            var debitTransaction = new Transaction(OperationType.DEBIT, 100L, "23");
            var creditTransaction = new Transaction(OperationType.CREDITING, 1000L, "253");
            var arrestTransaction = new Transaction(OperationType.ARREST, 0L, "23353t");

            var nowTimeStamp = Calendar.getInstance().getTimeInMillis();

            var debitDto = new TransactionDto(debitTransaction, nowTimeStamp);
            var creditDto = new TransactionDto(creditTransaction, nowTimeStamp + 200);
            var arrestDto = new TransactionDto(arrestTransaction, nowTimeStamp + 300);

            ProducerTransactionStore.TRANSACTIONS_FOR_SEND.addAll(Arrays.asList(debitDto, creditDto, arrestDto));
        }, 0, 6, TimeUnit.SECONDS);

        // запуск отправки транзакций
        scheduledExecutorService.scheduleAtFixedRate(new SendTransactionsTask(producerServiceForTransaction),
                5, 5, TimeUnit.SECONDS);

        // запуск отправки подтверждений
        scheduledExecutorService.scheduleAtFixedRate(new BackFlowTopicTransactionTask(producerServiceForAck, timeWindowMillis),
                10, 12, TimeUnit.SECONDS);

        // запуск потребителя транзакций
        executorService.execute(() -> new TransactionConsumerService(propertiesForTransactionConsumer).startListen());

        // запуск потребителя подтверждений
        executorService.execute(() -> new AckConsumerServiceImpl(propertiesForBackFlowConsumer).startListen());

        Thread.currentThread().join();
    }
}