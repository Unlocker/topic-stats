package ru.unlocker.topic.stats.config;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.unlocker.topic.stats.TopicDataProvider;

/**
 *
 * @author unlocker
 */
@Configuration
public class TestContext {

    @Bean
    public TopicDataProvider provider() {
        return Mockito.mock(TopicDataProvider.class);
    }
}
