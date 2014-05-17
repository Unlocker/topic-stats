package ru.unlocker.topic.stats.views;

/**
 * Обёртка исключения.
 *
 * @author unlocker
 */
public class ExceptionView {

    /**
     * Сообщение об ошибке.
     */
    private final String errorMessage;

    /**
     * Обёртка исключения.
     *
     * @param errorMessage сообщение об ошибке
     */
    public ExceptionView(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * @return сообщение об ошибке
     */
    public String getErrorMessage() {
        return errorMessage;
    }

}
