package org.refined.exceptions;

/// This is only thrown if the following occurs:
/// - The underlying thread was canceled/interrupted
/// - An uncaught-error was thrown. (Which is explicitly printed to console)
public final class JoinException extends RuntimeException {
    public JoinException(String message) {
        super(message);
    }
}
