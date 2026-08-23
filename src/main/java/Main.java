import org.refined.AsyncStream;
@SuppressWarnings({"unused"})
void main() {
    List<Integer> holder = IntStream.rangeClosed(1,5_000_000).boxed().toList();
    List<Long> results = new ArrayList<>(100);
    for (int i = 0; i < 500; i++) {
        long ms = System.currentTimeMillis();
        List<Integer> result =
            new AsyncStream<>(holder)
                .map(item -> item * 10)
                .toList();
        long current = System.currentTimeMillis();
        results.add(current - ms);
    }
    results.sort(Long::compareTo);
    for (Long result : results) {
        System.out.println(result + "ms");
    }
}
