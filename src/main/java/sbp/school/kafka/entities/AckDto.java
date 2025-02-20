package sbp.school.kafka.entities;

/**
 * DTO для сообщения подтверждения
 */
public class AckDto {
    /**
     * Начало временного промежутка формате Unix timestamp
     */
    private Long startTimeWindow;
    /**
     * Конец временного промежутка формате Unix timestamp
     */
    private Long endTimeWindow;
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
     * @param startTimeWindow начало временного промежутка
     * @param endTimeWindow конец временного промежутка
     * @param hash хеш-сумма
     */
    public AckDto(Long startTimeWindow, Long endTimeWindow, String hash) {
        this.startTimeWindow = startTimeWindow;
        this.endTimeWindow = endTimeWindow;
        this.hash = hash;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public Long getStartTimeWindow() {
        return startTimeWindow;
    }

    public void setStartTimeWindow(Long startTimeWindow) {
        this.startTimeWindow = startTimeWindow;
    }

    public Long getEndTimeWindow() {
        return endTimeWindow;
    }

    public void setEndTimeWindow(Long endTimeWindow) {
        this.endTimeWindow = endTimeWindow;
    }
}
