package org.refined.annotation_processor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/// Denotes a method/field/class/etc that was generated
/// artificially to create internal functionality.
///
/// Make note, there's a chance not all generated elements
/// will have this annotation, this is the fault of the
/// user, not me. Pester them to fix it if you run into
/// this issue.
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.TYPE, ElementType.METHOD,ElementType.CONSTRUCTOR,ElementType.FIELD})
public @interface Artificial {
    String value();
}
