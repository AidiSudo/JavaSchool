package sbp.school.kafka.entities;

/**
 * DTO для сообщения подтверждения
 */
public class AckDto {
    /**
     * Временная метка в формате Unix timestamp
     */
    private Long timestamp;
    /**
     * Хеш-сумма
     */
    private String hash;

    /**
     * ctor
     */
    public AckDto() {}

    /**
     * ctor
     *
     * @param timestamp временная метка
     * @param hash хеш-сумма
     */
    public AckDto(Long timestamp, String hash) {
        this.timestamp = timestamp;
        this.hash = hash;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }
}
