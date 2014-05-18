package ru.unlocker.topic.stats.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
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

    public static final String HISTORY_FOLDER_NAME = "history";
    public static final String TIMESTAMP_FOLDER_TEMPLATE = "YYYY-MM-dd-HH-mm-ss";
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public TopicParts getTopicParts(String topicId) throws TopicDataException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Фильтр папок в соответствии с шаблоном времени.
     */
    public static class TimestampFolderFilter implements DirectoryStream.Filter<Path> {

        @Override
        public boolean accept(Path entry) throws IOException {
            String filename = entry.getFileName().toString();
            return TIMESTAMP_REGEX_TEMPLATE.matcher(filename).matches()
                    && Files.isDirectory(entry);
        }
    }
}
