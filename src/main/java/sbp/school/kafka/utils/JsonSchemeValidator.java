package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Класс для проверок объекта на json схему
 */
public class JsonSchemeValidator {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);

    private static final Logger logger = LoggerFactory.getLogger(JsonSchemeValidator.class.getName());

    /**
     * Проверить объект по json схеме
     *
     * @param object объект
     * @param pathToJsonSchema путь до json файла валидацтт
     * @return валидность объекта
     */
    public static boolean isObjectValid(Object object, String pathToJsonSchema) {
        JsonNode json = objectMapper.valueToTree(object);
        JsonSchema schema = factory.getSchema(JsonSchemeValidator.class.getResourceAsStream(pathToJsonSchema));

        Set<ValidationMessage> validationResult = schema.validate(json);

        if (!validationResult.isEmpty()) {
            for (var validationMessage : validationResult) {
                logger.warn(validationMessage.getMessage());
            }
        }

        return validationResult.isEmpty();
    }
}
