package com.ksc.wordcount.shuffle.nettyimpl.client;


import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;
//存放我们的KeyValue对象
class BlockingQueueStream<T> {

    private BlockingQueue<T> queue;
    private boolean done = false;

    public BlockingQueueStream(int capacity) {
        queue = new LinkedBlockingQueue<T>(capacity);
    }


    public Stream<T> stream() {
        Spliterator<T> spliterator = new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {

            public boolean tryAdvance(Consumer<? super T> action) {
                while (true) {
                    if (done && queue.isEmpty()) {
                        return false;
                    }
                    T t = queue.poll();
//                    T t = queue.take();
                    if (t != null) {
                        action.accept(t);
                        return true;
                    }
                }
            }
        };
        return StreamSupport.stream(spliterator, false);
    }

    public void add(T t) {
//        queue.offer(t);
        try {
            queue.put(t);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void done() {
        done = true;
    }
}
