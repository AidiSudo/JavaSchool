package sbp.school.kafka.services;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

/**
 * Потребитель данных из брокера
 */
public abstract class BaseConsumerService<V> {
    protected final Consumer<String, V> consumer;

    protected static final Logger logger = LoggerFactory.getLogger(BaseConsumerService.class.getName());
    private java.util.function.Consumer<Throwable> exceptionConsumer;

    /**
     * ctor
     *
     * @param consumer потребитель
     */
    public BaseConsumerService(Consumer consumer, java.util.function.Consumer<Throwable> exceptionConsumer) {
        this.consumer = consumer;
        this.exceptionConsumer = exceptionConsumer;
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
            exceptionConsumer.accept(e);
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

    public void stop() {
        consumer.wakeup();
    }

    protected abstract String getTopicName();

    /**
     * Обработать записи
     *
     * @param records записи
     */
    protected abstract void processRecord(ConsumerRecords<String, V> records);
}
