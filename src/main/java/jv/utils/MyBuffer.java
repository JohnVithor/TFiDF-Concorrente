package jv.utils;

import java.util.LinkedList;
import java.util.Queue;

public class MyBuffer<T> {
    private final Queue<T> queue;
    private final long size;

    public MyBuffer(long size) {
        this.queue = new LinkedList<>();
        this.size = size;
    }

    public synchronized void put(T value) throws InterruptedException {
        while (queue.size() >= size) {
            wait();
        }
        queue.add(value);
        notify();
    }

    public synchronized T take() throws InterruptedException {
        while (queue.size() == 0) {
            wait();
        }
        T value = queue.poll();
        notify();
        return value;
    }

    public synchronized long size() {
        return queue.size();
    }
}
