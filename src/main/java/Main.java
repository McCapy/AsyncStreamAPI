import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() throws InterruptedException {
    Integer[] items = IntStream.rangeClosed(1,100_000).boxed().toArray(Integer[]::new);
    long start = System.currentTimeMillis();
    AsyncStream<Integer> result =
        new AsyncStream<>(items)
            .onStart(() -> System.out.println("Started"))
            .map(item -> item * 2)
            .onComplete(() -> System.out.println("Completed"))
            .onCancel(err -> {
                System.out.println("Dun Dun Dun");
            })
            .onCancel(() -> System.out.println("Other"))
            .start();
    Thread.sleep(5);
    result.cancel();
    System.out.println(result.toList());
    //System.out.println(result); // prints result (causes lag though because System.out.println() is inefficient)
}
