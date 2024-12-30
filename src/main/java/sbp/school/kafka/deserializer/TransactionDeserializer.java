package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.exceptions.InvalidFormatException;
import sbp.school.kafka.utils.JsonSchemeValidator;

import java.io.IOException;

/**
 * Десериализатор для транзакции
 */
public class TransactionDeserializer implements Deserializer<Transaction> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionDeserializer.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String deserializationException = "Ошибка при десериализации данных по транзакции";
    private static final String invalidFormat = "Транзакция с uuid %s не соответствует формату";

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        Transaction transaction;

        try {
            transaction = objectMapper.readValue(data, Transaction.class);
        } catch (IOException e) {
            logger.error(deserializationException, e);

            throw new SerializationException(deserializationException, e);
        }

        if (!JsonSchemeValidator.isObjectValid(transaction, "/transaction_schema.json")) {
            logger.error(String.format(invalidFormat, transaction.getUuid()));

            throw new InvalidFormatException(String.format(invalidFormat, transaction.getUuid()));
        }

        return transaction;
    }
}
