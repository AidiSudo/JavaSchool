package sbp.school.kafka.entities;

/**
 * Тип операции транзакции
 */
public enum OperationType {
    /**
     * Списание денег
     */
    DEBIT(0),
    /**
     * Зачисление денег
     */
    CREDITING(1),
    /*
     * Арест счета
     */
    ARREST(2);

    /**
     * Ключ операции
     */
    private int key;

    OperationType(int key) {
        this.key = key;
    }

    public int getOperationKey() {
        return key;
    }
}
