import org.refined.AsyncStream;

void main() { // test case
    Integer[] result =
        new AsyncStream<>(1,2,3,4,5)
            .submit(() -> System.out.println("Gather completed"))
            .forkEach(AsyncStream::new)
            .gather(Integer[].class)
            .toArray(Integer[]::new);
    System.out.println(Arrays.toString(result));
}