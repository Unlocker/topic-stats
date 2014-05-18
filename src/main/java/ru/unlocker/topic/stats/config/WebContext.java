package ru.unlocker.topic.stats.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.text.SimpleDateFormat;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import ru.unlocker.topic.stats.TopicDataException;
import ru.unlocker.topic.stats.TopicDataProvider;
import ru.unlocker.topic.stats.controllers.TopicsController;
import ru.unlocker.topic.stats.filesystem.FileSystemTopicDataProvider;

/**
 * Настройка контекста веб-приложения
 *
 * @author unlocker
 */
@Configuration
@EnableWebMvc
@ComponentScan(basePackageClasses = {TopicDataProvider.class, TopicsController.class})
public class WebContext extends WebMvcConfigurerAdapter {

    /**
     * Шаблон формата даты (ISO8601)
     */
    private static final String DATETIME_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    /**
     * корневая папка
     */
    private static String rootFolder;

    /**
     * @param rootFolder корневая папка
     */
    public static void setRootFolder(String rootFolder) {
        WebContext.rootFolder = rootFolder;
    }

    @Override
    public void configureMessageConverters(final List<HttpMessageConverter<?>> converters) {
        converters.add(0, jsonConverter());
    }

    /**
     * @return конвертер объектов в JSON
     */
    @Bean
    public MappingJackson2HttpMessageConverter jsonConverter() {
        final MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        mapper.setDateFormat(new SimpleDateFormat(DATETIME_FORMAT_PATTERN));
        converter.setObjectMapper(mapper);
        return converter;
    }

    /**
     * @return поставщик данных о топиках
     * @throws TopicDataException
     */
    @Bean
    public TopicDataProvider provider() throws TopicDataException {
        return new FileSystemTopicDataProvider(rootFolder);
    }

}
