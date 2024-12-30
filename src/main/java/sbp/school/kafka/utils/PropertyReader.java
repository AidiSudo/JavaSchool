package sbp.school.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс для чтения данных из .properties файлов
 */
public class PropertyReader {
    private static final Logger logger = LoggerFactory.getLogger(PropertyReader.class.getName());

    /**
     * Считать данный из .properties файла
     *
     * @param path путь к файлу
     * @return данные из файла
     * @throws IOException ошибка чтения данных
     */
    public static Properties readPropertiesFromFile(String path) throws IOException {
        try (InputStream inputStream = PropertyReader.class.getClassLoader().getResourceAsStream(path)) {
            Properties properties = new Properties();
            properties.load(inputStream);

            return properties;
        }
        catch (IOException e) {
            logger.error(String.format("Ошибка чтения файла %s", path), e);

            throw e;
        }
    }
}
