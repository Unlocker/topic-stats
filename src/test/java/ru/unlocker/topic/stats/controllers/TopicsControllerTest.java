package ru.unlocker.topic.stats.controllers;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.*;
import org.joda.time.DateTime;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import ru.unlocker.topic.stats.TopicDataException;
import ru.unlocker.topic.stats.TopicDataProvider;
import ru.unlocker.topic.stats.config.TestContext;
import ru.unlocker.topic.stats.views.TopicParts;
import ru.unlocker.topic.stats.views.TopicStats;

/**
 * Тесты контроллера топиков
 *
 * @author unlocker
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestContext.class})
@WebAppConfiguration
public class TopicsControllerTest {

    /**
     * Тип содержимого сервиса
     */
    public static final MediaType APPLICATION_JSON_UTF8 = new MediaType(
            MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype(),
            Charset.forName("utf8"));

    /**
     * MVC-mock
     */
    private MockMvc mockMvc;

    /**
     * Поставщик данных о топиках
     */
    @Autowired
    private TopicDataProvider provider;

    /**
     * Контекст web-приложения
     */
    @Autowired
    private WebApplicationContext webApplicationContext;

    /**
     * Предустановка тестов.
     */
    @Before
    public void setUp() {
        Mockito.reset(provider);
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    /**
     * Проверка возврата списка идентификаторов топиков
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnTopicsIds() throws Exception {
        // GIVEN
        final List<String> topics = Arrays.asList("a", "b", "c");
        when(provider.getTopics()).thenReturn(topics);
        // WHEN
        ResultActions result = mockMvc.perform(get("/topics"));
        // THEN
        verify(provider, times(1)).getTopics();
        verifyNoMoreInteractions(provider);
        result.andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$", Matchers.hasSize(3)))
                .andExpect(jsonPath("$[*]", Matchers.containsInAnyOrder("a", "b", "c")));
    }

    /**
     * Проверка обработки исключения, если контроллер его пробрасывает
     *
     * @throws Exception
     */
    @Test
    public void shouldWrapExceptionWhenThrown() throws Exception {
        // GIVEN
        final String topicId = "a";
        when(provider.getLastTopicTimestamp(topicId))
                .thenThrow(TopicDataException.noSuchTopicException(topicId));
        // WHEN
        ResultActions result = mockMvc.perform(get(String.format("/topics/%s/last", topicId)));
        // THEN
        ArgumentCaptor<String> topicIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(provider, times(1)).getLastTopicTimestamp(topicIdCaptor.capture());
        verifyNoMoreInteractions(provider);

        assertThat(topicIdCaptor.getValue(), is(topicId));
        result.andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$.errorMessage", not(isEmptyOrNullString())));
    }

    /**
     * Проверка получения времени последнего запуска топика
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnLastTimestampForTopic() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = DateTime.parse("2014-05-01");
        when(provider.getLastTopicTimestamp(topicId)).thenReturn(ts);
        // WHEN
        ResultActions result = mockMvc.perform(get(String.format("/topics/%s/last", topicId)));
        // THEN
        ArgumentCaptor<String> topicIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(provider, times(1)).getLastTopicTimestamp(topicIdCaptor.capture());
        verifyNoMoreInteractions(provider);

        assertThat(topicIdCaptor.getValue(), is(topicId));
        result.andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8));
    }
    
    /**
     * Проверка получения статистики последнего запуска топика
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnStatsForTopic() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = DateTime.parse("2014-05-01");
        final Integer val = 3;
        TopicStats stats = new TopicStats(topicId, ts, new Long(val), new Long(val), new Long(val));
        when(provider.getTopicStats(topicId)).thenReturn(stats);
        // WHEN
        ResultActions result = mockMvc.perform(get(String.format("/topics/%s/stats", topicId)));
        // THEN
        ArgumentCaptor<String> topicIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(provider, times(1)).getTopicStats(topicIdCaptor.capture());
        verifyNoMoreInteractions(provider);

        assertThat(topicIdCaptor.getValue(), is(topicId));
        result.andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$.id", is(topicId)))
                .andExpect(jsonPath("$.min", is(val)))
                .andExpect(jsonPath("$.max", is(val)))
                .andExpect(jsonPath("$.avg", is(val)));
    }
    
    /**
     * Проверка получения списка партиций для последнего запуска топика
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnPartsForTopic() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = DateTime.parse("2014-05-01");
        final Integer val = 3;
        Map<Integer, Long> partsMap = ImmutableMap.of(1, 2L, 3, 4L, 5, 6L);
        TopicParts parts = new TopicParts(topicId, ts, partsMap);
        when(provider.getTopicParts(topicId)).thenReturn(parts);
        // WHEN
        ResultActions result = mockMvc.perform(get(String.format("/topics/%s/parts", topicId)));
        // THEN
        ArgumentCaptor<String> topicIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(provider, times(1)).getTopicParts(topicIdCaptor.capture());
        verifyNoMoreInteractions(provider);

        assertThat(topicIdCaptor.getValue(), is(topicId));
        result.andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$.id", is(topicId)));
    }
}
