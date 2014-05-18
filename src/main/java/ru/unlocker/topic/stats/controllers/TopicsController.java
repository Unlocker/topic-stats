package ru.unlocker.topic.stats.controllers;

import java.util.Date;
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

    private TopicDataProvider provider;

    @Autowired
    public void setProvider(TopicDataProvider provider) {
        this.provider = provider;
    }

    /**
     * @return перечень топиков
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
     */
    @RequestMapping("/topics/{id}/last")
    @ResponseBody()
    public DateTime getTopicTimestamp(@PathVariable(value = "id") String id)
            throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException {
        final DateTime ts = provider.getLastTopicTimestamp(id);
        //return ISODateTimeFormat.dateTime().print(ts);
        return ts;
    }

    /**
     * Запрос статистики по топику
     *
     * @param id идентификатор
     * @return статистика
     */
    @RequestMapping("/topics/{id}/stats")
    @ResponseBody
    public TopicStats getTopicStats(@PathVariable(value = "id") String id) throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException {
        return provider.getTopicStats(id);
    }

    /**
     * Запрос списка партиций по топику
     *
     * @param id идентификатор
     * @return список партиций топика
     */
    @RequestMapping("/topics/{id}/parts")
    @ResponseBody
    public TopicParts getTopicParts(@PathVariable(value = "id") String id) throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException {
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
