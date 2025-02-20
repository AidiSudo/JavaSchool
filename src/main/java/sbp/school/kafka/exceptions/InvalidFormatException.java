package sbp.school.kafka.exceptions;

import org.apache.kafka.common.KafkaException;

/**
 * Исключение при неверном формате сообщения
 */
public class InvalidFormatException extends KafkaException {
    /**
     * ctor
     *
     * @param message сообщение
     */
    public InvalidFormatException(String message) {
        super(message);
    }
}
