import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    Optional<Integer[]> result =
        new AsyncStream<>(1,2,3,4,5)
            .submit(() -> {
                throw new RuntimeException("Dun Dun Dun");
            })
            .guard(self -> self
                .map(item -> {
                    if (item == 3) throw new RuntimeException("FAILED!");
                    return item * 2;
                })
                .filter(item -> item % 2 == 0)
            )
            .yield(err -> {
                err.printStackTrace();
                return Arrays.asList(5,4,3,2,1);
            })
            .toAbstract(item -> Optional.ofNullable(item.toArray(Integer[]::new)));
    System.out.println(result);
}
