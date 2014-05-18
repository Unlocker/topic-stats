package ru.unlocker.topic.stats.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.unlocker.topic.stats.TopicDataException;
import ru.unlocker.topic.stats.TopicDataProvider;
import ru.unlocker.topic.stats.views.TopicParts;
import ru.unlocker.topic.stats.views.TopicStats;

/**
 * Поставщик данных о топиках в файловой системе
 *
 * @author unlocker
 */
public class FileSystemTopicDataProvider implements TopicDataProvider {

    /**
     * Название папки с историей запуска топика.
     */
    public static final String HISTORY_FOLDER_NAME = "history";

    /**
     * Шаблон наименования папки отдельных запусков топика.
     */
    public static final String TIMESTAMP_FOLDER_TEMPLATE = "YYYY-MM-dd-HH-mm-ss";

    /**
     * Наименование файла CSV.
     */
    public static final String CSV_DATAFILE_NAME = "offsets.csv";

    /**
     * Шаблон временной метки.
     */
    private static final Pattern TIMESTAMP_REGEX_TEMPLATE = Pattern.compile("^\\d{4}(-\\d{2}){5}$");

    /**
     * Лог.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemTopicDataProvider.class);

    /**
     * Корневая папка.
     */
    private final File root;

    /**
     * Поставщик данных о топиках в файловой системе
     *
     * @param rootPath путь к корневой папке
     * @throws TopicDataException неправильный путь к корневой папке
     */
    public FileSystemTopicDataProvider(String rootPath) throws TopicDataException {
        File rootFile = new File(rootPath);
        if (!(rootFile.exists() && rootFile.isDirectory())) {
            throw new TopicDataException(String.format("Путь к корневой папке '%s' указан неправильно.", rootPath));
        }
        this.root = rootFile;
    }

    @Override
    public List<String> getTopics() throws TopicDataException {
        List<String> topics = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root.toPath())) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                    topics.add(entry.getFileName().toString());
                }
            }
            return topics;
        } catch (IOException ex) {
            final String message = "Ошибка получения списка топиков.";
            LOGGER.error(message, ex);
            throw new TopicDataException(message, ex);
        }
    }

    @Override
    public DateTime getLastTopicTimestamp(final String topicId) throws TopicDataException {
        Path topicDirPath = Paths.get(root.getPath(), topicId);
        if (Files.notExists(topicDirPath) || !Files.isDirectory(topicDirPath)) {
            throw TopicDataException.noSuchTopicException(topicId);
        }
        File historyDir = new File(topicDirPath.toFile(), HISTORY_FOLDER_NAME);
        if (!historyDir.exists() || historyDir.listFiles().length == 0) {
            throw TopicDataException.missingTopicDataException(topicId);
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(historyDir.toPath(),
                new TimestampFolderFilter())) {
            SortedSet<DateTime> allTimestamps = new TreeSet<>(DateTimeComparator.getInstance());
            for (Path entry : stream) {
                final String filename = entry.getFileName().toString();
                final DateTime ts = DateTime.parse(filename, DateTimeFormat.forPattern(TIMESTAMP_FOLDER_TEMPLATE));
                allTimestamps.add(ts);
            }
            if (allTimestamps.isEmpty()) {
                throw TopicDataException.missingTopicDataException(topicId);
            }
            return allTimestamps.last();
        } catch (IOException ex) {
            final String message = String.format("Ошибка получения времени запуска топика '%s'.", topicId);
            LOGGER.error(message, ex);
            throw new TopicDataException(message, ex);
        }
    }

    @Override
    public TopicStats getTopicStats(String topicId) throws TopicDataException {
        final DateTime last = getLastTopicTimestamp(topicId);
        Path csvPath = getCsvPath(topicId, last);
        try (Stream<String> stream = Files.lines(csvPath)) {
            CsvRowConsumer consumer = new CsvRowConsumer();
            stream.forEach(consumer);
            Map<Integer, Long> parts = consumer.getParts();
            if (parts.isEmpty()) {
                throw TopicDataException.missingTopicDataException(topicId);
            }
            return calculateStatsForTopic(topicId, last, parts);

        } catch (IOException ex) {
            final String message = String.format("Ошибка получения статистики топика '%s'.", topicId);
            LOGGER.error(message, ex);
            throw new TopicDataException(message, ex);
        }
    }

    @Override
    public TopicParts getTopicParts(String topicId) throws TopicDataException {
        final DateTime last = getLastTopicTimestamp(topicId);
        Path csvPath = getCsvPath(topicId, last);
        try (Stream<String> stream = Files.lines(csvPath)) {
            CsvRowConsumer consumer = new CsvRowConsumer();
            stream.forEach(consumer);
            return new TopicParts(topicId, last, consumer.getParts());

        } catch (IOException ex) {
            final String message = String.format("Ошибка получения списка партиций топика '%s'.", topicId);
            LOGGER.error(message, ex);
            throw new TopicDataException(message, ex);
        }
    }

    /**
     * Метод получения пути к csv-файлу.
     *
     * @param topicId идентификатор топика
     * @param ts временная метка
     * @return путь к файлу
     */
    private Path getCsvPath(String topicId, DateTime ts) {
        return Paths.get(root.toString(),
                topicId,
                HISTORY_FOLDER_NAME,
                ts.toString(TIMESTAMP_FOLDER_TEMPLATE),
                CSV_DATAFILE_NAME);
    }

    /**
     * Рассчитать статистику для топика.
     *
     * @param topicId идентификатор топика
     * @param last временная отметка
     * @param parts список партиций
     * @return статистика
     */
    private TopicStats calculateStatsForTopic(String topicId, DateTime last, Map<Integer, Long> parts) {
        long min, max, sum;
        Iterator<Long> iterator = parts.values().iterator();
        Long firstVal = iterator.next();
        min = firstVal;
        max = firstVal;
        sum = firstVal;
        while (iterator.hasNext()) {
            Long value = iterator.next();
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
        }
        return new TopicStats(topicId, last, min, max, sum / parts.size());
    }

    /**
     * Обработчик строк в файле CSV.
     */
    private static class CsvRowConsumer implements Consumer<String> {

        /**
         * Набор партиций.
         */
        final Map<Integer, Long> parts = new HashMap<>();

        /**
         * @return набор партиций
         */
        public Map<Integer, Long> getParts() {
            return parts;
        }

        @Override
        public void accept(String t) {
            String[] split = t.split(",");
            if (split.length != 2) {
                return;
            }
            try {
                Integer part = Integer.parseInt(split[0]);
                Long messageCount = Long.parseLong(split[1]);
                Long oldMessageCount = parts.get(part);

                if (oldMessageCount == null) {
                    parts.put(part, messageCount);
                } else {
                    parts.put(part, messageCount + oldMessageCount);
                }
            } catch (NumberFormatException e) {
                // Найдена некорректная строка
            }
        }

    }

    /**
     * Фильтр папок в соответствии с шаблоном времени.
     */
    private static class TimestampFolderFilter implements DirectoryStream.Filter<Path> {

        @Override
        public boolean accept(Path entry) throws IOException {
            String filename = entry.getFileName().toString();
            return TIMESTAMP_REGEX_TEMPLATE.matcher(filename).matches()
                    && Files.isDirectory(entry);
        }
    }
}
