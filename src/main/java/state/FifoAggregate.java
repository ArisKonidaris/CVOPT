package state;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

/**
 * This is a wrapper for a Queue, used by a {@link FifoAccumulator}
 *
 * @param <T> The type of the data tuple inserted in the queue
 */
public class FifoAggregate<T> implements Serializable {

    private Queue<T> fifo;

    public FifoAggregate() {
        this(new LinkedList<>());
    }

    public FifoAggregate(Queue<T> fifo) {
        this.fifo = fifo;
    }

    public void push(T element) {
        fifo.add(element);
    }

    public T pop() {
        return fifo.remove();
    }

    public boolean isEmpty() {
        return fifo.isEmpty();
    }

    public int size() {
        return fifo.size();
    }

    public Queue<T> getFifo() {
        return fifo;
    }

    public void setFifo(Queue<T> fifo) {
        this.fifo = fifo;
    }

}
