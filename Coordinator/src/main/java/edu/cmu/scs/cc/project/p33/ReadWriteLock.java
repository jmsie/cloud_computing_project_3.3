package edu.cmu.scs.cc.project.p33;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ReadWriteLock {
    private ConcurrentHashMap<String, PriorityQueue<Long>> keyLock = new ConcurrentHashMap<>();

    // Try to get the lock
    public synchronized void acquireLock(String key, Long timestamp) throws InterruptedException {
        PriorityQueue<Long> lock = null;
        if (keyLock.containsKey(key)) {
            lock = keyLock.get(key);
        } else {
            lock = new PriorityQueue<>();
            keyLock.put(key, lock);
        }
        // Insert the timestamp into the pq
        System.out.println("Acquire lock: " + key);
        lock.add(timestamp);
        // Wait if len(pq) != 0 and pq.head != key?

        while (!lock.peek().equals(timestamp)) {
            wait();
        }
        System.out.println("Got lock: " + key);
    }

    public synchronized void releaseLock(String key) {
        // Pop the first element in the pq
        // nodifyAll() to let other get the lock
        PriorityQueue<Long> lock = keyLock.get(key);
        System.out.println("Release lock: " + key);
        lock.poll();
        notifyAll();
    }


}