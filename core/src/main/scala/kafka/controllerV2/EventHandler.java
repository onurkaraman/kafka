package kafka.controllerV2;

import org.apache.kafka.common.utils.KafkaThread;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class EventHandler {
    private final EventHandlerThread _eventHandlerThread;

    public EventHandler() {
        _eventHandlerThread = new EventHandlerThread();
    }

    public void start() {
        _eventHandlerThread.start();
    }
    public void close() throws InterruptedException {
        _eventHandlerThread.interrupt();
        _eventHandlerThread.join();
    }
    public void pause() {
        _eventHandlerThread.pause();
    }
    public void unpause() {
        _eventHandlerThread.unpause();
    }
    public void clear() {
        _eventHandlerThread.clear();
    }
    public void add(Runnable runnable) {
        _eventHandlerThread.add(runnable);
    }

    /**
     * EventHandler composes a thread instead of inherits so that users won't be tempted to
     * accidentally call methods from Thread
     */
    private static class EventHandlerThread extends KafkaThread {
        private volatile boolean _isRunning;
        private final PausableBlockingQueue<Runnable> _pausableBlockingQueue;

        EventHandlerThread() {
            super("event-handler-thread", true);
            _isRunning = false;
            _pausableBlockingQueue = new PausableBlockingQueue<>();
        }

        public void pause() {
            _pausableBlockingQueue.pause();
        }
        public void unpause() {
            _pausableBlockingQueue.unpause();
        }
        public void clear() {
            _pausableBlockingQueue.clear();
        }
        public void add(Runnable runnable) {
            _pausableBlockingQueue.add(runnable);
        }

        @Override
        public void run() {
            _isRunning = true;
            while (_isRunning) {
                try {
                    Runnable runnable = _pausableBlockingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (runnable != null) {
                        runnable.run();
                    }
                } catch (InterruptedException e) {
                    _isRunning = false;
                    _pausableBlockingQueue.clear();
                }
            }
        }
    }
}

/**
 * A blocking queue that can be paused and unpaused. When paused, elements can be added to the queue but not polled out.
 */
class PausableBlockingQueue<E> {
    private final ReentrantLock _lock;
    private final Condition _maybeReady;
    private final Queue<E> _queue;
    private boolean _isPaused;

    public PausableBlockingQueue() {
        _lock = new ReentrantLock();
        _maybeReady = _lock.newCondition();
        _queue = new LinkedList<>();
        _isPaused = false;
    }

    public void pause() {
        _lock.lock();
        try {
            _isPaused = true;
        } finally {
            _lock.unlock();
        }
    }
    public void unpause() {
        _lock.lock();
        try {
            _isPaused = false;
            _maybeReady.signal();
        } finally {
            _lock.unlock();
        }
    }
    public void clear() {
        _lock.lock();
        try {
            _queue.clear();
        } finally {
            _lock.unlock();
        }
    }
    public void add(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        _lock.lock();
        try {
            _queue.add(e);
            _maybeReady.signal();
        } finally {
            _lock.unlock();
        }
    }

    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        long nanos = timeUnit.toNanos(timeout);
        _lock.lockInterruptibly();
        try {
            while (!hasNext()) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = _maybeReady.awaitNanos(nanos);
            }
            return _queue.poll();
        } finally {
            _lock.unlock();
        }
    }

    private boolean hasNext() {
        return !_isPaused && _queue.size() > 0;
    }
}