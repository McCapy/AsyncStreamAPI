import org.refined.AsyncStream;

// double check to make sure loop, flatmap, and forkEach/fork still work correctly (because i think i mugged them up)

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5,6,7,8,10)
            .map(holder -> new AsyncStream<>(holder).map(item -> item * 10))
            .flatMap(AsyncStream::toList)
            .toList();
    System.out.println(result);
}
