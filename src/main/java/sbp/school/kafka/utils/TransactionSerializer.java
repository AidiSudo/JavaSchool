package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import sbp.school.kafka.entities.Transaction;

/**
 * Класс для сериализации транзакции
 */
public class TransactionSerializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String serialize(Transaction transaction) throws JsonProcessingException {
        return MAPPER.writeValueAsString(transaction);
    }
}
