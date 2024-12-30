package sbp.school.kafka.services;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.utils.PropertyReader;

import java.util.Properties;

/**
 * Тестирование логики вычитки данных из брокера по транзакциям
 */
public class ConsumerServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerServiceTest.class.getName());
    private static Properties properties;

    @BeforeAll
    public static void init() {
        try {
            properties = PropertyReader.readPropertiesFromFile("consumer.properties");
        }
        catch (Exception e) {
            logger.error("Ошибка чтения конфигурации для потребителя кафка");

            return;
        }
    }

    @Test
    public void test() {
        ConsumerService consumerService = new ConsumerService(properties);

        consumerService.startListen(properties.getProperty("transactions.topic.name"));
    }
}