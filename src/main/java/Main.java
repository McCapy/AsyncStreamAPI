import org.refined.AsyncStream;

void main() { // test case
    Integer[] result =
        new AsyncStream<>(1,2,3,4,5)
            .forkEach(item ->
                new AsyncStream<>(item)
                    .map(other -> other * 5)
            )
            .collect(Integer.class,"2")
            .toArray(Integer[]::new);
    System.out.println(Arrays.toString(result));
}