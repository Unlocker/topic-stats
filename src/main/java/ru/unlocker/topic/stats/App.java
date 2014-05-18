package ru.unlocker.topic.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import ru.unlocker.topic.stats.config.WebContext;

/**
 * Точка входа приложения
 */
@ComponentScan
@EnableAutoConfiguration
public class App {

    /**
     * Логгер.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    /**
     * Точка входа приложения
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            LOGGER.error("Ошибка! Ожидался 1 параметр: путь к папке с топиками.");
            return;
        }
        try {
            WebContext.setRootFolder(args[0]);
            SpringApplication.run(App.class, args);
        } catch (Exception ex) {
            LOGGER.error("Ошибка запуска приложения.", ex);
        }
    }
}
