import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3)

            .toList();
    System.out.println(result);
}
