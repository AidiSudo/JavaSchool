package sbp.school.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Конфиг БД
 */
public class DbConfig extends AbstractConfig {
    public static final String URL_CONFIG = "hostname";
    public static final String PORT_CONFIG = "port";
    public static final String DB_CONFIG = "db";
    public static final String USERNAME_CONFIG = "username";
    public static final String PASSWORD_CONFIG = "password";
    public static final String TOPIC_CONFIG = "topic";
    public static final String MAX_BATCH_SIZE_CONFIG = "max.batch.size";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "hostname")
            .define(PORT_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "port")
            .define(DB_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "db name")
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "username")
            .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "password")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topic")
            .define(MAX_BATCH_SIZE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "max batch size");

    public DbConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}
