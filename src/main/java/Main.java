import org.refined.AsyncStream;

import java.time.Duration;
import java.util.stream.IntStream;

Integer[] items = IntStream.range(1,50_000_001).boxed().toArray(Integer[]::new);
void main1(int i) {
    long start = System.currentTimeMillis();
    System.out.println("Starting sequential...");
    String[] result = new String[items.length];
    for (int k = 0; k < items.length; k++) {
        result[k] = String.valueOf(items[k] * 10);
    }
    long end = System.currentTimeMillis() - start;
    System.out.println("Sequential completed in " + end + "ms.");
}

void main2(int i) {
    long start = System.currentTimeMillis();
    System.out.println("Starting parallel stream...");
    String[] result = IntStream.range(0, items.length)
            .parallel()
            .mapToObj(hold -> String.valueOf(items[hold] * 10))
            .toArray(String[]::new);
    long end = System.currentTimeMillis() - start;
    System.out.println("Parallel stream completed in " + end + "ms.");
}
void main3(int i) {
    long start = System.currentTimeMillis();
    System.out.println("Starting AsyncStream...");
    String[] result = new AsyncStream<>(items)
        .map(item -> String.valueOf(item * 10))
        .toArray(String[]::new);
    long end = System.currentTimeMillis() - start;
    System.out.println("AsyncStream " + (i) +  " completed in " + end + "ms.");
}

void main() throws InterruptedException {
    Thread.sleep(Duration.ofMillis(1000));
    for (int i = 1; i < 51; i++) {
        main1(i);
        main2(i);
        main3(i);
    }
}
