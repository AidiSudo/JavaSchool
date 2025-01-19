package sbp.school.kafka.services;

import sbp.school.kafka.entities.AckDto;

import java.util.Properties;

/**
 * Поставщик данных в кафку об подтверждениях
 */
public class AckProducerServiceImpl extends BaseProducerService<AckDto> {
    /**
     * ctor
     *
     * @param properties настройки
     */
    public AckProducerServiceImpl(Properties properties) {
        super(properties);
    }

    @Override
    protected void handleSuccess(AckDto ack) {
        logger.info(String.format("Запись c hash %s yспешно добавлена в брокер", ack.getHash()));
    }
}
