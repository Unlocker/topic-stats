package ru.unlocker.topic.stats;

import java.util.List;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import ru.unlocker.topic.stats.views.TopicParts;
import ru.unlocker.topic.stats.views.TopicStats;

/**
 * Поставщик данных о топиках
 *
 * @author unlocker
 */
@Service
public interface TopicDataProvider {

    /**
     * @return перечень топиков
     * @throws ru.unlocker.topic.stats.TopicDataException ошибка получения списка топиков
     */
    List<String> getTopics() throws TopicDataException;

    /**
     * Возвращает дату последнего запуска для топика
     *
     * @param topicId идентификатор топика
     * @return дата последнего запуска
     * @throws ru.unlocker.topic.stats.TopicDataException.NoSuchTopicException топик не найден
     * @throws ru.unlocker.topic.stats.TopicDataException.MissingTopicDataException нет данных по запускам топика
     */
    DateTime getLastTopicTimestamp(String topicId)
            throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException;

    /**
     * Получает статистику по топику
     *
     * @param topicId идентификатор топика
     * @return статистика топика
     * @throws ru.unlocker.topic.stats.TopicDataException.NoSuchTopicException топик не найден
     * @throws ru.unlocker.topic.stats.TopicDataException.MissingTopicDataException нет данных по запускам топика
     */
    TopicStats getTopicStats(String topicId)
            throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException;

    /**
     * Получает список партиций
     *
     * @param topicId идентификатор топика
     * @return список партиций
     * @throws ru.unlocker.topic.stats.TopicDataException.NoSuchTopicException топик не найден
     * @throws ru.unlocker.topic.stats.TopicDataException.MissingTopicDataException нет данных по запускам топика
     */
    TopicParts getTopicParts(String topicId)
            throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException;
}
