import org.refined.AsyncStream;
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
            .parallel(8,item -> item * 10)
            .replace(item -> item % 3 == 0,999)
            .filter(item -> item % 3 == 0)
            .toList();
    System.out.println(result);
}