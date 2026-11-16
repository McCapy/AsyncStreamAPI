package org.refined.annotation_processor;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD,ElementType.METHOD,ElementType.TYPE})
public @interface Generates {
    @NotNull String[] imports() default {};
}
