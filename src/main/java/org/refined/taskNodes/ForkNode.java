package org.refined.taskNodes;

import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ForkNode<T,A> implements TaskNode<T> {

    final String id;
    final Class<T> expectedClass;
    final AsyncStream<T> stream;

    public ForkNode(String id, Class<T> expectedClass, AsyncStream<T> stream) {
        this.id = id;
        this.expectedClass = expectedClass;
        this.stream = stream;
    }

    @Override
    public Class<ForkNode> getType() {
        return ForkNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) { // im such a chud
        stream.scope().setTask(0,new OfferNode<>((A[]) scope.getItems()));
        scope.identityMap.put(id,(Map.Entry<AsyncStream<Object>, Class<Object>>) (Object) Map.entry(stream,expectedClass));
        stream.start();
        return null;
    }
}
