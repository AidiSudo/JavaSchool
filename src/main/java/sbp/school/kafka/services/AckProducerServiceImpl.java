package sbp.school.kafka.services;

import org.apache.kafka.clients.producer.Producer;
import sbp.school.kafka.entities.AckDto;
import sbp.school.kafka.utils.Constants;

/**
 * Поставщик данных в кафку об подтверждениях
 */
public class AckProducerServiceImpl extends BaseProducerService<AckDto> {
    /**
     * ctor
     *
     * @param producer поставщик данных
     */
    public AckProducerServiceImpl(Producer producer) {
        super(producer);
    }

    @Override
    protected String getTopicName() {
        return Constants.BACK_FLOW_TOPIC;
    }

    @Override
    protected void handleSuccess(AckDto ack) {
        logger.info(String.format("Запись c hash %s yспешно добавлена в брокер", ack.getHash()));
    }
}
