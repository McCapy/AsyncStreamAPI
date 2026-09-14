package org.refined.annotation_processor;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.*;

/// This annotation was designed to do a single thing.
/// To generate code.
/// The single parameter it takes, a string, and inserts
/// all the (valid) code provided into the AsyncStream
/// class, to see this class run
/// `mvn clean install`
/// which will generate the class. You can insert fields,
/// classes, methods, and more.
///
/// The only catch is: you should annotate anything you
/// make with: @Artificial(String message) this just lets
/// users know that it was generated externally.
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD,ElementType.ANNOTATION_TYPE,ElementType.TYPE})
public @interface Generates {
    @NotNull String value();
}
