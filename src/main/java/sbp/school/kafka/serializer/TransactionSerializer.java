package sbp.school.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.TransactionDto;

import java.nio.charset.StandardCharsets;

/**
 * Сериализатор для транзакций
 */
public class TransactionSerializer implements Serializer<TransactionDto> {
    private static final Logger logger = LoggerFactory.getLogger(sbp.school.kafka.serializer.TransactionSerializer.class.getName());

    @Override
    public byte[] serialize(String topic, TransactionDto data) {
        if (data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                String value = objectMapper.writeValueAsString(data);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

                return valueBytes;
            } catch (JsonProcessingException e) {
                logger.error("Ошибка при сериализации данных по транзакций", e);

                throw new SerializationException(e);
            }
        }
        return null;
    }
}