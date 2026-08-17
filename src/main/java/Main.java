import org.refined.AsyncStream;

void main() { // test case
    Integer[] result =
        new AsyncStream<>(1,2,3,4,5)
            .forkEach(item ->
                new AsyncStream<>(item)
                    .map(other -> {
                        int i = other * 5;
                        System.out.println(i);
                        return i;
                    })
            )
            .collect(Integer[]::new,List.of("0","1","2","3","4"))
            .toArray(Integer[]::new);
    System.out.println(Arrays.toString(result));
}