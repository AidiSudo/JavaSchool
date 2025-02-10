package sbp.school.kafka.connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entities.OperationType;
import sbp.school.kafka.entities.Transaction;
import sbp.school.kafka.utils.TransactionSchema;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Таска, которая вычитывает данные из БД по транзакциям
 */
public class CustomTransactonDbSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(CustomTransactonDbSourceTask.class);

    private static final String SELECT_SQL =
            "SELECT * FROM TRANSACTIONS WHERE offset > ? ORDER BY offset LIMIT ?";
    private static final String TABLE_FIELD = "table";
    private static final String TRANSACTION_TABLE = "transaction";
    private static final String OFFSET = "offset";

    private static final String CONNECTION_CLOSE_ERROR = "Ошибка загрузки данных по траназкциям из БД";
    private static final String ERROR_CONNECTION = "Ошибка при подключении к БД";

    private Connection connection;

    private String topic;
    private int maxBatchSize;

    @Override
    public String version() {
        return new CustomTransactionDbSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        DbConfig config = new DbConfig(props);
        try {
            connection = DriverManager.getConnection(
                    String.format("%s:%s/%s",
                            props.get(DbConfig.URL_CONFIG),
                            props.get(DbConfig.PORT_CONFIG),
                            props.get(DbConfig.DB_CONFIG)),
                    props.get(DbConfig.USERNAME_CONFIG),
                    props.get(DbConfig.PASSWORD_CONFIG));

            topic = config.getString(DbConfig.TOPIC_CONFIG);
            maxBatchSize = config.getInt(DbConfig.MAX_BATCH_SIZE_CONFIG);
        } catch (Exception e) {
            LOG.error(ERROR_CONNECTION, e);

            throw new RuntimeException(ERROR_CONNECTION, e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(SELECT_SQL)) {

            Map<String, Object> sourcePartition = Collections.singletonMap(TABLE_FIELD, TRANSACTION_TABLE);
            Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
            long lastOffset = 0L;

            if (offset != null && offset.get(OFFSET) != null) {
                lastOffset = (Long) offset.get(OFFSET);
            }

            statement.setLong(1, lastOffset);
            statement.setInt(2, maxBatchSize);

            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                Transaction transaction = new Transaction(
                        rs.getString("uuid"),
                        OperationType.valueOf(rs.getString("operation_type")),
                        rs.getLong("sum"),
                        rs.getString("account")
                );

                long currentOffset = rs.getLong(OFFSET);

                Map<String, Long> sourceOffset = Collections.singletonMap(OFFSET, currentOffset);

                records.add(new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        TransactionSchema.SCHEMA,
                        TransactionSchema.getStruct(transaction)
                ));
            }
        } catch (SQLException e) {
            LOG.error(ERROR_CONNECTION, e);

            throw new RuntimeException(ERROR_CONNECTION, e);
        }

        return records;
    }

    @Override
    public void stop() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.error(CONNECTION_CLOSE_ERROR, e);

            throw new RuntimeException(CONNECTION_CLOSE_ERROR, e);
        }
    }
}
