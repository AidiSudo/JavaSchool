package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.utils.Constants;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Потребитель данных из брокера
 */
public abstract class BaseConsumerService<V> {
    private final KafkaConsumer<String, V> consumer;

    protected static final Logger logger = LoggerFactory.getLogger(BaseConsumerService.class.getName());
    protected final Properties properties;

    /**
     * ctor
     *
     * @param properties проперти
     */
    public BaseConsumerService(Properties properties) {
        this.consumer = new KafkaConsumer<>(properties);
        this.properties = properties;
    }

    /**
     * Начать прослушивать сообщения из брокера
     */
    public void startListen() {
        this.consumer.subscribe(Collections.singletonList(getTopicName()));

        try {
            while (true) {
                ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(100));

                processRecord(records);

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

    protected String getTopicName() {
        return properties.getProperty(Constants.TOPIC_PROPERTY_NAME);
    }

    /**
     * Обработать записи
     *
     * @param records записи
     */
    protected abstract void processRecord(ConsumerRecords<String, V> records);
}
