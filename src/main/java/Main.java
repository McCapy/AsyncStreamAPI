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
    /*
    make AsyncStages contain each other for compact execution, add error handling
    cancellation handling, as well as completion handling be self contained inside
    the tasknodes to prevent the blowup of the streamscope, from there im going to
    merge the streamscope into the AsynchronousStream so i dont have to worry about
    the existence of the third file, and then maybe do some extra stuff to the
    AsyncStage like converting it into fields rather, rather then doing list caches
    This will lower the amount of param lists that i have to mess with (which just..
    makes it more convenient to make asyncstages/tasknodes)
     */
}
