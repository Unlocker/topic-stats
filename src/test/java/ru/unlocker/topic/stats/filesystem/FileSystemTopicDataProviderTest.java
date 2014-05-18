package ru.unlocker.topic.stats.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import static org.hamcrest.Matchers.*;
import org.joda.time.DateTime;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import ru.unlocker.topic.stats.TopicDataException;
import ru.unlocker.topic.stats.TopicDataProvider;
import ru.unlocker.topic.stats.views.TopicParts;
import ru.unlocker.topic.stats.views.TopicStats;

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

    /**
     * Проверка возврата времени запуска топика
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnTimestampForTopic() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = new DateTime(2014, 5, 1, 5, 43);
        Path dirPath = Paths.get(rootDir.toString(),
                topicId,
                FileSystemTopicDataProvider.HISTORY_FOLDER_NAME,
                ts.toString(FileSystemTopicDataProvider.TIMESTAMP_FOLDER_TEMPLATE));
        dirPath = Files.createDirectories(dirPath);
        Files.createFile(Paths.get(dirPath.toString(), FileSystemTopicDataProvider.CSV_DATAFILE_NAME));
        FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        DateTime lastTs = provider.getLastTopicTimestamp(topicId);
        // THEN
        assertThat(lastTs, notNullValue());
        assertThat(lastTs, is(ts));
    }

    /**
     * Проверка возврата времени последнего запуска топика
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnLastTimestampForTopic() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = new DateTime(2014, 5, 1, 5, 43);
        // Создаём дополнительные отметки
        for (int i = 0; i < 3; i++) {
            DateTime anotherTs = ts.minusDays(i);
            Path dirPath = Paths.get(rootDir.toString(),
                    topicId,
                    FileSystemTopicDataProvider.HISTORY_FOLDER_NAME,
                    anotherTs.toString(FileSystemTopicDataProvider.TIMESTAMP_FOLDER_TEMPLATE));
            dirPath = Files.createDirectories(dirPath);
            Files.createFile(Paths.get(dirPath.toString(), FileSystemTopicDataProvider.CSV_DATAFILE_NAME));
        }
        FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        DateTime lastTs = provider.getLastTopicTimestamp(topicId);
        // THEN
        assertThat(lastTs, notNullValue());
        assertThat(lastTs, is(ts));
    }

    /**
     * Проверка возвращения всех партиций, если в файле нет дубликатов.
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnAllPartsForTopicWithoutDuplicates() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = new DateTime(2014, 5, 1, 5, 43);
        Path dirPath = Paths.get(rootDir.toString(),
                topicId,
                FileSystemTopicDataProvider.HISTORY_FOLDER_NAME,
                ts.toString(FileSystemTopicDataProvider.TIMESTAMP_FOLDER_TEMPLATE));
        Files.createDirectories(dirPath);
        writeFileFromResources("normal.csv", dirPath);
        FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        TopicParts parts = provider.getTopicParts(topicId);
        // THEN
        assertThat(parts, notNullValue());
        assertThat(parts.getId(), is(topicId));
        assertThat(parts.getTimestamp(), is(ts));
        assertThat(parts.getParts(), notNullValue());
        assertThat(parts.getParts().size(), is(5));
        assertThat(parts.getParts().get(1), is(100L));
        assertThat(parts.getParts().get(2), is(200L));
        assertThat(parts.getParts().get(3), is(300L));
        assertThat(parts.getParts().get(4), is(400L));
        assertThat(parts.getParts().get(5), is(500L));
    }

    /**
     * Проверка возвращения только уникальных партиций, если в файле есть дубликаты.
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnUniquePartsForTopicWithDuplicates() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = new DateTime(2014, 5, 1, 5, 43);
        Path dirPath = Paths.get(rootDir.toString(),
                topicId,
                FileSystemTopicDataProvider.HISTORY_FOLDER_NAME,
                ts.toString(FileSystemTopicDataProvider.TIMESTAMP_FOLDER_TEMPLATE));
        Files.createDirectories(dirPath);
        writeFileFromResources("duplicate.csv", dirPath);
        FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        TopicParts parts = provider.getTopicParts(topicId);
        // THEN
        assertThat(parts, notNullValue());
        assertThat(parts.getId(), is(topicId));
        assertThat(parts.getTimestamp(), is(ts));
        assertThat(parts.getParts(), notNullValue());
        assertThat(parts.getParts().size(), is(1));
        assertThat(parts.getParts().get(5), is(500L));
    }

    /**
     * Проверка статистики топика без дубликатов
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnTopicStatsForTopicWithoutDuplicates() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = new DateTime(2014, 5, 1, 5, 43);
        Path dirPath = Paths.get(rootDir.toString(),
                topicId,
                FileSystemTopicDataProvider.HISTORY_FOLDER_NAME,
                ts.toString(FileSystemTopicDataProvider.TIMESTAMP_FOLDER_TEMPLATE));
        Files.createDirectories(dirPath);
        writeFileFromResources("normal.csv", dirPath);
        FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        TopicStats stats = provider.getTopicStats(topicId);
        // THEN
        assertThat(stats, notNullValue());
        assertThat(stats.getId(), is(topicId));
        assertThat(stats.getTimestamp(), is(ts));
        assertThat(stats.getMin(), is(100L));
        assertThat(stats.getMax(), is(500L));
        assertThat(stats.getAvg(), is(300L));
    }

    /**
     * Проверка статистики топика с дубликатами
     *
     * @throws Exception
     */
    @Test
    public void shouldReturnTopicStatsForTopicWithDuplicates() throws Exception {
        // GIVEN
        final String topicId = "a";
        final DateTime ts = new DateTime(2014, 5, 1, 5, 43);
        Path dirPath = Paths.get(rootDir.toString(),
                topicId,
                FileSystemTopicDataProvider.HISTORY_FOLDER_NAME,
                ts.toString(FileSystemTopicDataProvider.TIMESTAMP_FOLDER_TEMPLATE));
        Files.createDirectories(dirPath);
        writeFileFromResources("duplicate.csv", dirPath);
        FileSystemTopicDataProvider provider = new FileSystemTopicDataProvider(rootDir.toString());
        // WHEN
        TopicStats stats = provider.getTopicStats(topicId);
        // THEN
        assertThat(stats, notNullValue());
        assertThat(stats.getId(), is(topicId));
        assertThat(stats.getTimestamp(), is(ts));
        assertThat(stats.getMin(), is(500L));
        assertThat(stats.getMax(), is(500L));
        assertThat(stats.getAvg(), is(500L));
    }

    /**
     * Записывает файл CSV из ресурсов.
     *
     * @param resource наименование ресурса
     * @param dir папка назначения
     * @throws IOException
     */
    private void writeFileFromResources(String resource, Path dir) throws IOException {
        URL resourceUrl = Thread.currentThread().getContextClassLoader().getResource(resource);
        File resourceFile = new File(resourceUrl.getPath());
        Path csvPath = Files.createFile(Paths.get(dir.toString(),
                FileSystemTopicDataProvider.CSV_DATAFILE_NAME));

        try (InputStream input = new FileInputStream(resourceFile);
                OutputStream output = new FileOutputStream(csvPath.toFile())) {

            int read;
            byte[] bytes = new byte[1024];
            while ((read = input.read(bytes)) != -1) {
                output.write(bytes, 0, read);
            }
        }
    }
}
