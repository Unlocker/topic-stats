package ru.unlocker.topic.stats;

/**
 * Базовое исключение приложения.
 *
 * @author unlocker
 */
public class TopicDataException extends Exception {

    /**
     * Исключение, если запрошенный топик не найден.
     *
     * @param topicId идентификатор топика
     * @return исключение
     */
    public static TopicDataException noSuchTopicException(String topicId) {
        final String template = "Топик с идентификатором '%s' не найден.";
        return new NoSuchTopicException(String.format(template, topicId));
    }

    /**
     * Исключение, если запрошенный топик не найден.
     *
     * @param topicId идентификатор топика
     * @return исключение
     */
    public static TopicDataException missingTopicDataException(String topicId) {
        final String template = "Данные по запускам топика '%s' отсутствуют.";
        return new MissingTopicDataException(String.format(template, topicId));
    }

    /**
     * Базовое исключение приложения.
     */
    public TopicDataException() {
    }

    /**
     * Базовое исключение приложения.
     *
     * @param message
     */
    public TopicDataException(String message) {
        super(message);
    }

    /**
     * Базовое исключение приложения.
     *
     * @param message
     * @param cause
     */
    public TopicDataException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Исключение, если запрошенный топик не найден.
     */
    public static class NoSuchTopicException extends TopicDataException {

        /**
         * Исключение, если запрошенный топик не найден.
         *
         * @param message
         */
        private NoSuchTopicException(String message) {
            super(message);
        }

    }

    /**
     * Исключение, если нет данных по запускам топика.
     */
    public static class MissingTopicDataException extends TopicDataException {

        /**
         * Исключение, если нет данных по запускам топика.
         *
         * @param message
         */
        private MissingTopicDataException(String message) {
            super(message);
        }

    }
}
