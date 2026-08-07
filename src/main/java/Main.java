import org.refined.AsyncStream;

void main() {
    // new error handling (allows for defaults, lowering the need for ifNull catches)
    // error catching is **NO LONGER REQUIRED** originally it would ruin your code,
    // forcing you to both catch implicitly, but also do a ifNull check, this is
    // no longer needed, as you can offer a value to replace the null on error.
    AsyncStream<Integer> stream =
        new AsyncStream<>(1,2,3,4,5)
            .map((item) -> {
                if (item == 3) {
                    throw new RuntimeException("Item cannot be three."); // throws return with null
                }
                return item + 1; // so the results of this get disregarded.
            })
            .catchError(() -> new Integer[]{10,9,8,7,6}) // this just offers a value on error
            .catchError((_) -> {
                // do smth w/ error
            })
            .catchError() // cancels on error
            .catchError(_ -> {
                // do smth w/ error
                return new Integer[]{10,9,8,7,6};
            }) // offers a value and does smth with the error
            .catchError(() -> {
                System.out.println("Smth");
                System.out.println("Smth else");
            })
            .start();
    System.out.println("Result joined from child thread: " + Arrays.toString(stream.join()));
}

void main2() {
    // new fork handling
    AsyncStream<String> stream =
        new AsyncStream<>(items)
            .forkEach(holder ->
                new AsyncStream<>(holder)
                    .map(String::valueOf)
            )
            .gather(String.class)
            .peek(System.out::println)
            .start();
    System.out.print("\nResult joined from child thread: " + Arrays.toString(stream.join()));
}
static final Integer[] items =
        new Integer[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};