import org.refined.AsyncStream;

void main() { // test case
    Integer[] result =
        new AsyncStream<>(1,2,3,4,5)
            .forkEach(item ->
                new AsyncStream<>(item)
                    .map(other -> other * 10)
            )
            .gather(Integer[].class)
            .catchError(err -> {
                err.printStackTrace();
            })
            .peek(item -> System.out.print(item + " "))
            .toArray();
}