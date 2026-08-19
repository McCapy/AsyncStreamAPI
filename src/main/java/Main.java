import org.refined.AsyncStream;

void main() { // test case
    String[] result =
        new AsyncStream<>(1,2,3,4,5)
            .map(String::valueOf)
            //.filter(item -> item % 2 == 0)
            .toArray(String[]::new);
    System.out.println(Arrays.toString(result));
}