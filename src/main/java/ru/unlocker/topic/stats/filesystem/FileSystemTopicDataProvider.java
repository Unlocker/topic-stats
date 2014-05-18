package ru.unlocker.topic.stats.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
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
    public DateTime getLastTopicTimestamp(String topicId) throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public TopicStats getTopicStats(String topicId) throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public TopicParts getTopicParts(String topicId) throws TopicDataException.NoSuchTopicException, TopicDataException.MissingTopicDataException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
