package ru.unlocker.topic.stats.views;

import java.util.Map;
import org.joda.time.DateTime;

/**
 * Список партиций и число сообщений в них.
 *
 * @author unlocker
 */
public class TopicParts {

    /**
     * идентификатор
     */
    private final String id;

    /**
     * дата последнего запуска
     */
    private final DateTime timestamp;

    /**
     * партиции и число сообщений
     */
    private final Map<Integer, Long> parts;

    /**
     *
     * @param id идентификатор
     * @param timestamp дата последнего запуска
     * @param parts партиции и число сообщений
     */
    public TopicParts(String id, DateTime timestamp, Map<Integer, Long> parts) {
        this.id = id;
        this.timestamp = timestamp;
        this.parts = parts;
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
     * @return партиции и число сообщений
     */
    public Map<Integer, Long> getParts() {
        return parts;
    }
    
}
