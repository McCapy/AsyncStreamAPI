package org.refined.annotation_processor;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.auto.service.AutoService;

@SuppressWarnings("unchecked")
@SupportedAnnotationTypes({"org.refined.annotation_processor.Insertion"})
@SupportedSourceVersion(SourceVersion.RELEASE_26)
@AutoService(Processor.class)
public class InsertionProcessor extends AbstractProcessor {
    private static String result =
            """
            package org.refined;
            
            import java.time.Duration;
            import java.util.Collection;
            import java.util.Comparator;
            import java.util.List;
            import java.util.concurrent.Executor;
            import java.util.function.*;
            
            @SuppressWarnings({"unchecked","unused"})
            public final class AsyncStream<T> extends org.refined.AsynchronousStream<T> {
            
                public AsyncStream() {
                    super();
                }
            
                public AsyncStream(T... values) {
                    super(values);
                }
            
                public AsyncStream(Collection<T> collection) {
                    super(collection);
                }
            
                @Override
                public <R> AsyncStream<R> of(Collection<R> collection) {
                    return new AsyncStream<>(collection);
                }
            
                @Override
                public <R> AsyncStream<R> of(R... values) {
                    return new AsyncStream<>(values);
                }
            
                @Override
                public AsyncStream<Void> ofEmpty() {
                    return new AsyncStream<>();
                }
            
                @Override
                public AsyncStream<T> yield(Consumer<RuntimeException> consumer) {
                    return (AsyncStream<T>) super.yield(consumer);
                }
            
                @Override
                public AsyncStream<T> yield(Function<RuntimeException, List<T>> fn) {
                    return (AsyncStream<T>) super.yield(fn);
                }
            
                @Override
                public <R> AsyncStream<R> guard(Function<org.refined.AsynchronousStream<T>, org.refined.AsynchronousStream<R>> fn) {
                    return (AsyncStream<R>) super.guard(fn);
                }
            
                @Override
                public AsyncStream<T> start() {
                    return (AsyncStream<T>) super.start();
                }
            
                @Override
                public AsyncStream<T> start(Executor executor) { return (AsyncStream<T>) super.start(executor); }
            
                @Override
                public AsyncStream<T> cancel() {
                    return (AsyncStream<T>) super.cancel();
                }
            
                @Override
                public AsyncStream<T> filter(Predicate<T> predicate) {
                    return (AsyncStream<T>) super.filter(predicate);
                }
            
                @Override
                public <R> AsyncStream<R> map(Function<T, R> function) {
                    return (AsyncStream<R>) super.map(function);
                }
            
                @Override
                public <R> AsyncStream<R> offer(R... items) {
                    return (AsyncStream<R>) super.offer(items);
                }
            
                @Override
                public <R> AsyncStream<R> offer(Collection<R> items) {
                    return (AsyncStream<R>) super.offer(items);
                }
            
                @Override
                public AsyncStream<T> offer(Function<List<T>, List<T>> function) {
                    return (AsyncStream<T>) super.offer(function);
                }
            
                @Override
                public AsyncStream<Void> empty(Runnable runnable) {
                    return (AsyncStream<Void>) super.empty(runnable);
                }
            
                @Override
                public AsyncStream<Void> empty() {
                    return (AsyncStream<Void>) super.empty();
                }
            
                @Override
                public AsyncStream<Void> empty(Consumer<List<T>> consumer) {
                    return (AsyncStream<Void>) super.empty(consumer);
                }
            
                @Override
                public <R> AsyncStream<R> flatMap(Function<T, List<R>> function) {
                    return (AsyncStream<R>) super.flatMap(function);
                }
            
                @Override
                public AsyncStream<T> parallelSort(Comparator<T> comparator) {
                    return (AsyncStream<T>) super.parallelSort(comparator);
                }
            
                @Override
                public AsyncStream<T> sort(Comparator<T> comparator) {
                    return (AsyncStream<T>) super.sort(comparator);
                }
            
                @Override
                public <R> AsyncStream<R> parallel(Function<T, R> mapper) {
                    return (AsyncStream<R>) super.parallel(mapper);
                }
            
                @Override
                public AsyncStream<Void> forEach(Consumer<T> consumer) {
                    return (AsyncStream<Void>) super.forEach(consumer);
                }
            
                @Override
                public AsyncStream<T> peek(Consumer<T> consumer) {
                    return (AsyncStream<T>) super.peek(consumer);
                }
            
                @Override
                public AsyncStream<T> loop(int repetitions, Function<List<T>, org.refined.AsynchronousStream<T>> stream) {
                    return (AsyncStream<T>) super.loop(repetitions, stream);
                }
            
                @Override
                public AsyncStream<T> submit(Runnable runnable) {
                    return (AsyncStream<T>) super.submit(runnable);
                }
            
                @Override
                public AsyncStream<T> delay(Duration duration) {
                    return (AsyncStream<T>) super.delay(duration);
                }
            
                @Override
                public AsyncStream<T> reversed() {
                    return (AsyncStream<T>) super.reversed();
                }
            
                @Override
                public AsyncStream<T> replace(Predicate<T> predicate, T replacement) {
                    return (AsyncStream<T>) super.replace(predicate, replacement);
                }
            
                @Override
                public AsyncStream<T> replace(Predicate<T> predicate, Supplier<T> replacement) {
                    return (AsyncStream<T>) super.replace(predicate, replacement);
                }
                //%s
            }""";
    private static final String TEMPLATE =
            """
            public <R> AsyncStream<R> %s(%s) {
                %s
                return (AsyncStream<R>) this;
            }
            //%s
            """;
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        List<? extends Element> validatedMethods =
            roundEnv.getElementsAnnotatedWith(Insertion.class).stream()
                .filter(clazz -> clazz.getKind().isClass())
                .toList();
        for (ExecutableElement validatedMethod : (List<ExecutableElement>) validatedMethods) {
            processingEnv.getMessager().printNote("Moving to next validated method.");
            StringBuilder methodParameters = new StringBuilder();
            for (VariableElement parameter : validatedMethod.getParameters()) {
                methodParameters.append(parameter.asType().toString()).append(" ").append(parameter.getSimpleName()).append(",");
            }
            String joinedNames = validatedMethod.getParameters().stream()
                    .map(param -> param.getSimpleName().toString())
                    .collect(Collectors.joining(","));
            processingEnv.getMessager().printWarning(result);
            result = result.formatted(
              TEMPLATE.formatted(
                  validatedMethod.getAnnotation(Insertion.class).value(),
                        methodParameters.toString(),
                        ((TypeElement)validatedMethod.getEnclosingElement()).getQualifiedName() + "." + validatedMethod.getSimpleName() + "(%s)".formatted(joinedNames),
                        "//%s"
                    )
            );
            processingEnv.getMessager().printWarning(result);
        }
        if (roundEnv.processingOver()) {
            try (Writer writer = processingEnv.getFiler().createSourceFile("org.refined.AsyncStream").openWriter()){
                writer.write(result);
            } catch (IOException _) {}
        }
        return true;
    }
}