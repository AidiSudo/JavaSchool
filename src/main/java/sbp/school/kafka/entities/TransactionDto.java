package sbp.school.kafka.entities;

/**
 * Dto транзакции
 */
public class TransactionDto {
    public TransactionDto() {}

    public TransactionDto(Transaction transaction, Long timestamp) {
        this.transaction = transaction;
        this.timestamp = timestamp;
    }

    /**
     * Транзакции
     */
    private Transaction transaction;
    /**
     * Timestamp в мс
     */
    private Long timestamp;

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
