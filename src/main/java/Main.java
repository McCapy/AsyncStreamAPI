import org.refined.AsyncStream;
import org.refined.TaskNode;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .test(items ->
                new AsyncStream<>(items)
                    .map(item -> {
                        if (item == 4) throw new TaskNode.StreamInterruption("Error");
                        return item;
                    })
            )
            .fail(err -> {
                System.out.println("In fail");
                err.printStackTrace();
                return Arrays.asList(1,2,3);
            })
            .toList();
    System.out.println(result);
}
