package ru.unlocker.topic.stats.controllers;

import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import ru.unlocker.topic.stats.TopicDataException;
import ru.unlocker.topic.stats.TopicDataProvider;
import ru.unlocker.topic.stats.views.ExceptionView;
import ru.unlocker.topic.stats.views.TopicParts;
import ru.unlocker.topic.stats.views.TopicStats;

/**
 * Контроллер топиков
 *
 * @author unlocker
 */
@Controller
public class TopicsController {

    /**
     * Поставщик данных о топиках.
     */
    private TopicDataProvider provider;

    /**
     * @param provider поставщик данных о топиках
     */
    @Autowired
    public void setProvider(TopicDataProvider provider) {
        this.provider = provider;
    }

    /**
     * @return перечень топиков
     * @throws ru.unlocker.topic.stats.TopicDataException ошибка получения списка топиков
     */
    @RequestMapping("/topics")
    @ResponseBody
    public List<String> getTopics() throws TopicDataException {
        return provider.getTopics();
    }

    /**
     * Запрос даты последнего запуска для топика
     *
     * @param id идентификатор топика
     * @return дата последнего запуска
     * @throws ru.unlocker.topic.stats.TopicDataException.NoSuchTopicException топика не существует
     * @throws ru.unlocker.topic.stats.TopicDataException.MissingTopicDataException топика ни разу не запускался
     */
    @RequestMapping("/topics/{id}/last")
    @ResponseBody()
    public DateTime getTopicTimestamp(@PathVariable(value = "id") String id) throws TopicDataException {
        return provider.getLastTopicTimestamp(id);
    }

    /**
     * Запрос статистики по топику
     *
     * @param id идентификатор
     * @return статистика
     * @throws ru.unlocker.topic.stats.TopicDataException.NoSuchTopicException топика не существует
     * @throws ru.unlocker.topic.stats.TopicDataException.MissingTopicDataException топика ни разу не запускался
     */
    @RequestMapping("/topics/{id}/stats")
    @ResponseBody
    public TopicStats getTopicStats(@PathVariable(value = "id") String id) throws TopicDataException {
        return provider.getTopicStats(id);
    }

    /**
     * Запрос списка партиций по топику
     *
     * @param id идентификатор
     * @return список партиций топика
     * @throws ru.unlocker.topic.stats.TopicDataException.NoSuchTopicException топика не существует
     * @throws ru.unlocker.topic.stats.TopicDataException.MissingTopicDataException топика ни разу не запускался
     */
    @RequestMapping("/topics/{id}/parts")
    @ResponseBody
    public TopicParts getTopicParts(@PathVariable(value = "id") String id) throws TopicDataException {
        return provider.getTopicParts(id);
    }

    /**
     * Обработчик исключений.
     *
     * @param ex исключение
     * @param request запрос
     * @return представление исключения
     */
    @ExceptionHandler(Exception.class)
    @ResponseBody
    public ExceptionView handleException(Exception ex, HttpServletRequest request) {
        return new ExceptionView(ex.getMessage());
    }
}
