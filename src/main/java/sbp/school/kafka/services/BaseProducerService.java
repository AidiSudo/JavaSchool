package sbp.school.kafka.services;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Поставщик данных в кафку
 */
public abstract class BaseProducerService<V> {
    private final Producer<String, V> producer;

    protected static final Logger logger = LoggerFactory.getLogger(BaseProducerService.class.getName());

    /**
     * ctor
     *
     * @param producer поставщик
     */
    public BaseProducerService(Producer producer) {
        this.producer = producer;
    }

    /**
     * Отправить данные в топик
     *
     * @param key ключ
     * @param value значение
     */
    public void send(String key, V value) {
        try {
            producer.send(new ProducerRecord<>(getTopicName(), key, value),
                    ((metadata, exception) -> {
                        if (exception != null) {
                            logger.error(String.format("Ошибка записи данных в топик %s, в partition = %s, offset = %s",
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset()));
                        } else {
                            handleSuccess(value);
                        }
                    }));
        } finally {
            producer.flush();
        }
    }

    protected abstract String getTopicName();

    protected abstract void handleSuccess(V value);
}
