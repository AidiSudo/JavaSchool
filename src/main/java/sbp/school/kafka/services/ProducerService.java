package sbp.school.kafka.services;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.Transaction;

import java.util.Properties;

/**
 * Поставщик данных в кафку
 */
public class ProducerService {
    private final KafkaProducer<String, Transaction> producer;

    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class.getName());

    /**
     * @param properties проперти
     */
    public ProducerService(Properties properties) {
        this.producer = new KafkaProducer<>(properties);
    }

    /**
     * Отправить данные об транзакции в топик
     */
    public void sendTransaction(Transaction transaction, String topicName) {
        producer.send(new ProducerRecord<>(topicName, transaction.getUuid(), transaction),
                ((metadata, exception) -> {
                    if (exception != null) {
                        logger.error(String.format("Ошибка записи данных в partition = %s, offset = %s",
                                        metadata.topic(),
                                        metadata.partition(),
                                        metadata.offset()));
                    } else {
                        logger.info(String.format("Запись c UUID %s успешно добавлена в брокер", transaction.getUuid()));
                    }
                }));

        producer.flush();
    }
}
