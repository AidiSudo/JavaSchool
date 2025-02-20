package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.AckDto;

import java.io.IOException;

/**
 * Десериализатор для подтверждения
 */
public class AckDeserializer implements Deserializer<AckDto> {
    private static final Logger logger = LoggerFactory.getLogger(AckDeserializer.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String deserializationException = "Ошибка при десериализации данных по подтверждению";

    @Override
    public AckDto deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, AckDto.class);
        } catch (IOException e) {
            logger.error(deserializationException, e);

            throw new SerializationException(deserializationException, e);
        }
    }
}
