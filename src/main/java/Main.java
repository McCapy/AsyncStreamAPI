import org.refined.AsyncStream;

// double check to make sure loop, flatmap, and forkEach/fork still work correctly (because i think i mugged them up)

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5,6,7,8,10)
            .map(item -> {
                if (item == 5) throw new RuntimeException("Failed");
                return item * 2;
            })
            .intercept((err,scope) -> {
                err.printStackTrace();
                return List.of(1,2,3,4,5);
            })
            .toList();
    System.out.println(result);
}
