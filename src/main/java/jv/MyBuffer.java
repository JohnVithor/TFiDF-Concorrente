package jv;

import java.util.LinkedList;
import java.util.Queue;

public class MyBuffer<T> {
    private final Queue<T> queue;
    private final long size;
    private final Object hasSomething = new Object();
    private final Object hasSpace = new Object();

    public MyBuffer(long size) {
        this.queue = new LinkedList<>();
        this.size = size;
    }

    public synchronized void put(T value) throws InterruptedException {
        while (queue.size() >= size) {
            hasSpace.wait();
        }
        queue.add(value);
        hasSomething.notify();
    }

    public synchronized T take() throws InterruptedException {
        while (queue.size() == 0) {
            hasSomething.wait();
        }
        T value = queue.poll();
        hasSpace.notify();
        return value;
    }
}
