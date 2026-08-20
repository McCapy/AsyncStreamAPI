import org.refined.AsyncStream;

void main() { // test case
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .map(item -> item * 10)
            .forkEach(item ->
                new AsyncStream<>(item)
                    .map(other -> other * 10)
            )
            .gather(Integer.class)
            .flatMap(item -> item)
            .toList();
    System.out.println(result);
}