package main;

import java.util.LinkedList;
import java.util.Queue;

public class Buffer<T> {

    private final Queue<T> list;
    private final int size;

    public Buffer(int size) {
        this.list = new LinkedList<>();
        this.size = size;
    }

    public void add(T value) throws InterruptedException {
        synchronized (this) {
            while (list.size() >= size) {
                wait();
            }
            list.add(value);
            notify();
        }
    }

    public T poll() throws InterruptedException {
        synchronized (this) {
            while (list.size() == 0) {
                wait();
            }
            T value = list.poll();
            notify();
            return value;
        }
    }
}
