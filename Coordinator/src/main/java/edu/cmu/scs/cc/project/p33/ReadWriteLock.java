package edu.cmu.scs.cc.project.p33;

import java.time.LocalDate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class ReadWriteLock {
    private ConcurrentHashMap<String, PriorityBlockingQueue<Long>> keyLock = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> readers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> writers = new ConcurrentHashMap<>();

    public synchronized void addLock(String key, Long timestamp) {
        PriorityBlockingQueue<Long> lock = null;
        if (keyLock.containsKey(key)) {
            lock = keyLock.get(key);
        } else {
            lock = new PriorityBlockingQueue<>();
            keyLock.put(key, lock);
            writers.put(key, new Integer(0));
            readers.put(key, new Integer(0));
        }
        // Insert the timestamp into the pq
        lock.add(timestamp);
    }

    // Try to get the lock
    public synchronized void acquireLock(String key, Long timestamp) throws InterruptedException {
        addLock(key, timestamp);
        PriorityBlockingQueue<Long> lock = keyLock.get(key);
        while (!lock.peek().equals(timestamp)) {
            wait();
        }
    }

    public synchronized void releaseLock(String key, Long timestamp) {
        // Pop the first element in the pq
        // nodifyAll() to let other get the lock
        PriorityBlockingQueue<Long> lock = keyLock.get(key);
        //lock.poll();
        lock.remove(timestamp);
        notifyAll();
    }

    public synchronized void acquireReadLock(String key, Long timestamp) throws InterruptedException {
        addLock(key, timestamp);
        PriorityBlockingQueue<Long> lock = keyLock.get(key);
        Integer writer = writers.get(key);
        Integer reader = readers.get(key);
        while (!lock.peek().equals(timestamp) || writer > 0) {
            wait();
        }
        reader++;

    }
    public synchronized void acquireWriteLock(String key, Long timestamp) throws InterruptedException {
        addLock(key, timestamp);
        PriorityBlockingQueue<Long> lock = keyLock.get(key);
        Integer writer = writers.get(key);
        Integer reader = readers.get(key);
        while (!lock.peek().equals(timestamp) || reader > 0) {
            wait();
        }
        writer++;
    }

    public synchronized void releaseReadLock(String key, Long timestamp) {
        PriorityBlockingQueue<Long> lock = keyLock.get(key);
        //lock.poll();
        lock.remove(timestamp);
        Integer reader = readers.get(key);
        reader--;
        notifyAll();
    }

    public synchronized void releaseWriteLock(String key, Long timestamp) {
        PriorityBlockingQueue<Long> lock = keyLock.get(key);
        //lock.poll();
        lock.remove(timestamp);
        Integer writer = readers.get(key);
        writer--;
        notifyAll();
    }

}