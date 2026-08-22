import org.refined.AsyncStream;
@SuppressWarnings({"unused"})
void main() {
    List<Integer> holder = IntStream.rangeClosed(1,1_000).boxed().toList();
    long ms = System.currentTimeMillis();
    List<Integer> result =
        new AsyncStream<>(holder)
            .toList();
    System.out.println(System.currentTimeMillis() - ms + "ms");
    System.out.println(result);
}