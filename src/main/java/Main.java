import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .guard()
            .map(item -> {
                if (item == 3) throw new RuntimeException("FAILED!");
                return item * 2;
            })
            .submit(() -> System.out.println("Completed."))
            .yield(err -> {
                err.printStackTrace();
                return Arrays.asList(5,4,3,2,1);
            })
            .toList();
    System.out.println(result);
}
