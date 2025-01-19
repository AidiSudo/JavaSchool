package sbp.school.kafka.entities;

import java.util.Calendar;
import java.util.UUID;

/**
 * Транзакция
 */
public class Transaction {
    /**
     * Идентификатор транзакции
     */
    private String uuid;
    /**
     * Тип операции
     */
    private OperationType operationType;
    /**
     * Сумма транзакции
     */
    private long sum;
    /**
     * Банковский счет
     */
    private String account;
    /**
     * Дата операции
     */
    private Calendar date;

    public Transaction() {}

    public Transaction(OperationType operationType, long sum, String account) {
        this.uuid = UUID.randomUUID().toString();
        this.operationType = operationType;
        this.sum = sum;
        this.account = account;
        this.date = Calendar.getInstance();
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public Calendar getDate() {
        return date;
    }

    public void setDate(Calendar date) {
        this.date = date;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}
