import org.refined.AsyncStream;
import org.refined.annotation_processor.Generates;

@Generates("""
           public AsyncStream<T> stream() {
               return this;
           }
           """)
public class Tester {
    public static void main(String[] args) {
        AsyncStream<Integer> stream =
            new AsyncStream<>(1,2,3,4,5)
                .map(item -> item * 2)
                .start();
        System.out.println(stream.toList());
    }
}
