import org.refined.AsyncStream;

void main() throws InterruptedException {
    AsyncStream<Integer> stream =
        new AsyncStream<>(1, 2, 3, 4, 5)
            .forkEach(
                new AsyncStream<Integer>()
                    .map(item -> item * 2)
            )
            .collect("0", Integer.class)
            .empty(item -> System.out.println(item))
            .collect("1", Integer.class)
            .empty(item -> System.out.println(item))
            .collect("2", Integer.class)
            .empty(item -> System.out.println(item))
            .collect("3", Integer.class)
            .empty(item -> System.out.println(item))
            .collect("4", Integer.class)
            .empty(item -> System.out.println(item))
            .offer(new Integer[]{null})
            .start();
    Thread.sleep(6_000L);
    System.out.print("Result joined from child thread: " + Arrays.toString(stream.join()));
}