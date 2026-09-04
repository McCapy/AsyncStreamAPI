import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10)
            .map(item -> item * 10)
            .toList();
    System.out.println(result);
    /*
    make AsyncStages contain each other for compact execution, add error handling
    cancellation handling, as well as completion handling be self-contained inside
    the task nodes to prevent the blowup of the stream scope, from there I'm going to
    merge the stream scope into the AsynchronousStream, so I don't have to worry about
    the existence of the third file, and then maybe do some extra stuff to the
    AsyncStage like converting it into fields rather, rather than doing list caches
    This will lower the amount of param lists that I have to mess with (which just…
    makes it more convenient to make asyncstages/tasknodes)
     */
}
