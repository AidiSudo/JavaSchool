package sbp.school.kafka.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import sbp.school.kafka.entities.Transaction;

/**
 * Схема для транзакции
 */
public class TransactionSchema {
    public static final Schema SCHEMA = SchemaBuilder.struct()
            .field("uuid", Schema.STRING_SCHEMA)
            .field("operationType", Schema.STRING_SCHEMA)
            .field("sum", Schema.INT64_SCHEMA)
            .field("account", Schema.STRING_SCHEMA)
            .build();

    public static Struct getStruct(Transaction transaction) {
        Struct struct = new Struct(SCHEMA);
        struct.put("uuid", transaction.getUuid());
        struct.put("operation_type", transaction.getOperationType().toString());
        struct.put("sum", transaction.getSum());
        struct.put("account", transaction.getAccount());

        return struct;
    }
}
