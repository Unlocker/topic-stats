package ru.unlocker.topic.stats.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import ru.unlocker.topic.stats.TopicDataException;
import ru.unlocker.topic.stats.TopicDataProvider;

/**
 * Тесты работы поставщика данных по топикам
 *
 * @author unlocker
 */
public class FileSystemTopicDataProviderTest {

    /**
     * Префикс для временных файлов.
     */
    private static final String TEMP_FILE_PREFIX = "topic-stats";

    /**
     * Корневая папка.
     */
    private Path rootDir;

    /**
     * Установка
     *
     * @throws IOException
     */
    @Before
    public void setUp() throws IOException {
        this.rootDir = Files.createTempDirectory(TEMP_FILE_PREFIX);
    }

    /**
     * Очистка
     *
     * @throws IOException
     */
    @After
    public void tearDown() throws IOException {
        if (Files.exists(rootDir)) {
            FileUtils.forceDelete(rootDir.toFile());
        }
        rootDir = null;
    }

    /**
     * Проверка выброса исключения, если указанный путь не указывает на папку.
     *
     * @throws Exception
     */
    @Test
    public void shouldThrowExceptionWhenFileDefinedAsRoot() throws Exception {
        // GIVEN
        Path tmp = Files.createTempFile(TEMP_FILE_PREFIX, ".tmp");
        try {
            // WHEN
            FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(tmp.toString());
            fail("Ожидалось исключение.");
        } catch (Exception ex) {
            // THEN
            assertThat(ex, instanceOf(TopicDataException.class));
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /**
     * Проверка возврата списка топиков
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnAListOfTopics() throws Exception {
        // GIVEN
        List<String> topics = Arrays.asList("a", "b", "c");
        for (String topic : topics) {
            Files.createDirectory(Paths.get(rootDir.toString(), topic));
        }
        TopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        List<String> actualTopics = provider.getTopics();
        // THEN
        assertThat(actualTopics, notNullValue());
        assertThat(actualTopics.size(), is(3));
        assertThat(actualTopics, containsInAnyOrder(topics.toArray()));
    }

}
