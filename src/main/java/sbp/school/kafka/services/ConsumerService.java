package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.Transaction;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Потребитель данных из брокера
 */
public class ConsumerService {
    private final KafkaConsumer<String, Transaction> consumer;

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class.getName());

    /**
     * ctor
     *
     * @param properties проперти
     */
    public ConsumerService(Properties properties) {
        this.consumer = new KafkaConsumer<>(properties);
    }

    /**
     * Начать прослушивать сообщения из брокера
     */
    public void startListen(String topic) {
        this.consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(100));

                for (var record : records) {
                    processRecord(record);
                }

                consumer.commitAsync();
            }
        }
        catch (Exception e) {
            logger.error("Ошибка обработки сообщений из брокера", e);
        }
        finally {
            try {
                consumer.commitSync();
            }
            finally {
                consumer.close();
            }
        }
    }

    private void processRecord(ConsumerRecord<String, Transaction> record) {
        logger.trace(String.format("Получена запись по транзакции c ключом %s из партиции %n из топика %s",
                record.value().getUuid(), record.partition(), record.topic()));
    }
}
