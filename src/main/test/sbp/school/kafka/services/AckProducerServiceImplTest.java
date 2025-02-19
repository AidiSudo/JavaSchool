package sbp.school.kafka.services;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entities.AckDto;
import sbp.school.kafka.serializer.AckSerializer;

/**
 * Тесты для {@link AckProducerServiceImpl}
 */
class AckProducerServiceImplTest {
    private MockProducer<String, AckDto> producer;
    private AckProducerServiceImpl producerService;

    private static final AckDto ackDto = new AckDto(100L, 200L, "MD5");

    @BeforeEach
    public void setUp() {
        this.producer = new MockProducer(true, new StringSerializer(), new AckSerializer());
        this.producerService = new AckProducerServiceImpl(producer);
    }

    /**
     * Кейс успешной отправки сообщения
     */
    @Test
    void sendSuccess() {
        producerService.send(ackDto.getHash(), ackDto);

        AckDto sendedAck = producer.history().get(0).value();

        Assertions.assertEquals(sendedAck.getHash(), ackDto.getHash());
        Assertions.assertEquals(sendedAck.getStartTimeWindow(), ackDto.getStartTimeWindow());
        Assertions.assertEquals(sendedAck.getEndTimeWindow(), ackDto.getEndTimeWindow());

        producer.clear();
    }
}