package ru.unlocker.topic.stats.views;

import org.joda.time.DateTime;

/**
 * Статистика по топику
 *
 * @author unlocker
 */
public class TopicStats {

    /**
     * идентификатор
     */
    private final String id;

    /**
     * дата последнего запуска
     */
    private final DateTime timestamp;

    /**
     * минимальное количество сообщений
     */
    private final Long min;

    /**
     * максимальное количество сообщений
     */
    private final Long max;

    /**
     * среднее количество сообщений
     */
    private final Long avg;

    /**
     * Статистика по топику
     *
     * @param id идентификатор
     * @param timestamp дата последнего запуска
     * @param min минимальное количество сообщений
     * @param max максимальное количество сообщений
     * @param avg среднее количество сообщений
     */
    public TopicStats(String id, DateTime timestamp, Long min, Long max, Long avg) {
        this.id = id;
        this.timestamp = timestamp;
        this.min = min;
        this.max = max;
        this.avg = avg;
    }

    /**
     * @return идентификатор
     */
    public String getId() {
        return id;
    }

    /**
     * @return дата последнего запуска
     */
    public DateTime getTimestamp() {
        return timestamp;
    }

    /**
     * @return минимальное количество сообщений
     */
    public Long getMin() {
        return min;
    }

    /**
     * @return максимальное количество сообщений
     */
    public Long getMax() {
        return max;
    }

    /**
     * @return среднее количество сообщений
     */
    public Long getAvg() {
        return avg;
    }

}
