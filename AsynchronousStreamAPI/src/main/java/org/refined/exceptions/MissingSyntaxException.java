package org.refined.exceptions;


/// This is only thrown if the following occurs, no other scenarios.
/// - An unaccompanied guard or yield is found.
public final class MissingSyntaxException extends RuntimeException {
    public MissingSyntaxException(String message, Throwable cause) {
        super(message, cause);
    }
}
