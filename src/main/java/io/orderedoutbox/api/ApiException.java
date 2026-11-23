package io.orderedoutbox.api;

public abstract class ApiException extends RuntimeException {

    protected ApiException(String message) {
        super(message);
    }

    public abstract int status();

    public static final class BadRequest extends ApiException {
        public BadRequest(String message) {
            super(message);
        }

        @Override
        public int status() {
            return 400;
        }
    }

    public static final class NotFound extends ApiException {
        public NotFound(String message) {
            super(message);
        }

        @Override
        public int status() {
            return 404;
        }
    }

    public static final class Conflict extends ApiException {
        public Conflict(String message) {
            super(message);
        }

        @Override
        public int status() {
            return 409;
        }
    }
}
