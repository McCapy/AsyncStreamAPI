import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .onStart(() -> System.out.println("Started"))
            .map(item -> item * 2)
            .onComplete(() -> System.out.println("Completed"))
            .onCancel(err -> {
                err.printStackTrace();
                System.out.println("Dun Dun Dun");
            })
            .toList();
    System.out.println(result);
}
