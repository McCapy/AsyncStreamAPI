package org.refined.annotation_processor;

import com.google.auto.service.AutoService;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.Writer;
import java.util.Set;

@SupportedAnnotationTypes({"org.refined.annotation_processor.Generates"})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
@AutoService(Processor.class)
public class GenerationProcessor extends AbstractProcessor {
    private static String result = """
     package org.refined;
     
     import org.refined.annotation_processor.Artificial;
     
     import java.time.Duration;
     import java.util.Collection;
     import java.util.Comparator;
     import java.util.List;
     import java.util.concurrent.Executor;
     import java.util.function.*;
     
     @Artificial("See documentation.")
     public final class AsyncStream<T> extends org.refined.AsynchronousStream<T> {
     %s
     }
     """;
    private static JavaFileObject file;

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(Generates.class)) result = result.formatted(element.getAnnotation(Generates.class).value() + "\n    %s");
        if (file != null) file.delete();
        try (Writer writer = (file = processingEnv.getFiler().createSourceFile("org.refined.AsyncStream")).openWriter()) {
            writer.write(result.formatted(""));
        } catch (IOException ignored) {}
        return true;
    }
}
