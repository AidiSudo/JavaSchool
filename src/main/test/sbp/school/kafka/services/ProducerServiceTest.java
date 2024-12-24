package sbp.school.kafka.services;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.OperationType;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.utils.JsonSchemeValidator;
import sbp.school.kafka.utils.PropertyReader;

import java.util.Properties;

/**
 * Тестирование логики отправки данных в топик транзакций
 */
public class ProducerServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(ProducerServiceTest.class.getName());

    private static final String pathToJsonSchema = "/transaction_schema.json";

    private static ProducerService producerService;
    private static Properties properties;

    @BeforeClass
    public static void init() {
        try {
            properties = PropertyReader.readPropertiesFromFile("producer.properties");

            producerService = new ProducerService(properties);
        }
        catch (Exception e) {
            logger.error("Ошибка инициализаци сервиса отправки сообщений в брокер", e);

            return;
        }
    }

    @Test
    public void testDebitTransaction() {
        Transaction transaction = new Transaction(OperationType.DEBIT,
                100L,
                "456-567");

        send(transaction);
    }

    @Test
    public void testCreditingTransaction() {
        Transaction transaction = new Transaction(OperationType.CREDITING,
                200L,
                "456-758");

        send(transaction);
    }

    @Test
    public void testArrestTransaction() {
        Transaction transaction = new Transaction(OperationType.ARREST,
                -1L,
                "456-768");

        send(transaction);
    }

    private void send(Transaction transaction) {
        if (!JsonSchemeValidator.isObjectValid(transaction, pathToJsonSchema)) {
            logger.error(String.format("Транзакция с uuid %s не будет отправлена в топик, так как не соответствует формату",
                    transaction.getUuid()));

            return;
        }

        producerService.sendTransaction(transaction, properties.getProperty("transactions.topic.name"));
    }
}