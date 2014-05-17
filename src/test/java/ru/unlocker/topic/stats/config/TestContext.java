package ru.unlocker.topic.stats.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import ru.unlocker.topic.stats.TopicDataProvider;
import ru.unlocker.topic.stats.controllers.TopicsController;

/**
 *
 * @author unlocker
 */
@Configuration
@EnableWebMvc
@ComponentScan(basePackageClasses = {TopicDataProvider.class, TopicsController.class})
public class TestContext {

    @Bean
    public TopicDataProvider provider() {
        return Mockito.mock(TopicDataProvider.class);
    }
}
