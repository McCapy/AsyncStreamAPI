import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10)
            .map(item -> item * 10)
            .loop(10,(items) ->
                new AsyncStream<>(items)
                    .map(item -> item + 1)
            )
            .fork("example_id",items ->
                new AsyncStream<>(items)
                    .map(item -> item * 2)
            )
            .collect(Integer.class,List.of("example_id"))
            .toList();
    System.out.println(result);
}
