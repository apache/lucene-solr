//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.solr.common;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduledThreadPoolExecutor extends ParWorkExecutor implements
    ScheduledExecutorService {
  private volatile boolean continueExistingPeriodicTasksAfterShutdown;
  private volatile boolean executeExistingDelayedTasksAfterShutdown = true;
  volatile boolean removeOnCancel;
  private static final AtomicLong sequencer = new AtomicLong();
  private static final long DEFAULT_KEEPALIVE_MILLIS = 10L;

  boolean canRunInCurrentRunState(RunnableScheduledFuture<?> task) {
    if (!this.isShutdown()) {
      return true;
    } else {
      return task.isPeriodic() ? this.continueExistingPeriodicTasksAfterShutdown : this.executeExistingDelayedTasksAfterShutdown || task.getDelay(
          TimeUnit.NANOSECONDS) <= 0L;
    }
  }

  private void delayedExecute(RunnableScheduledFuture<?> task) {
    if (this.isShutdown()) {
      throw new RejectedExecutionException();
    } else {
      super.getQueue().add(task);
      if (!this.canRunInCurrentRunState(task) && this.remove(task)) {
        task.cancel(false);
      } else {
        prestartAllCoreThreads();
      }
    }

  }

  void reExecutePeriodic(RunnableScheduledFuture<?> task) {
    if (this.canRunInCurrentRunState(task)) {
      super.getQueue().add(task);
      if (this.canRunInCurrentRunState(task) || !this.remove(task)) {
        prestartAllCoreThreads();
        return;
      }
    }

    task.cancel(false);
  }

  void onShutdown() {
    BlockingQueue<Runnable> q = super.getQueue();
    boolean keepDelayed = this.getExecuteExistingDelayedTasksAfterShutdownPolicy();
    boolean keepPeriodic = this.getContinueExistingPeriodicTasksAfterShutdownPolicy();
    Object[] var4 = q.toArray();
    int var5 = var4.length;

    for(int var6 = 0; var6 < var5; ++var6) {
      Object e = var4[var6];
      if (e instanceof RunnableScheduledFuture) {
        RunnableScheduledFuture t;
        label28: {
          t = (RunnableScheduledFuture)e;
          if (t.isPeriodic()) {
            if (!keepPeriodic) {
              break label28;
            }
          } else if (!keepDelayed && t.getDelay(TimeUnit.NANOSECONDS) > 0L) {
            break label28;
          }

          if (!t.isCancelled()) {
            continue;
          }
        }

        if (q.remove(t)) {
          t.cancel(false);
        }
      }
    }

   // shutdown();
  }

  protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
    return task;
  }

  protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
    return task;
  }

  public ScheduledThreadPoolExecutor(String name) {
    super(name, 1, 1, 10, new ScheduledThreadPoolExecutor.DelayedWorkQueue());
  }


  private long triggerTime(long delay, TimeUnit unit) {
    return this.triggerTime(unit.toNanos(delay < 0L ? 0L : delay));
  }

  long triggerTime(long delay) {
    return System.nanoTime() + (delay < 4611686018427387903L ? delay : this.overflowFree(delay));
  }

  private long overflowFree(long delay) {
    Delayed head = (Delayed)super.getQueue().peek();
    if (head != null) {
      long headDelay = head.getDelay(TimeUnit.NANOSECONDS);
      if (headDelay < 0L && delay - headDelay < 0L) {
        delay = 9223372036854775807L + headDelay;
      }
    }

    return delay;
  }

  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    if (command != null && unit != null) {
      RunnableScheduledFuture<Void> t = this.decorateTask((Runnable)command, new ScheduledThreadPoolExecutor.ScheduledFutureTask(command, (Object)null, this.triggerTime(delay, unit), sequencer.getAndIncrement()));
      this.delayedExecute(t);
      return t;
    } else {
      throw new NullPointerException();
    }
  }

  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    if (callable != null && unit != null) {
      RunnableScheduledFuture<V> t = this.decorateTask((Callable)callable, new ScheduledThreadPoolExecutor.ScheduledFutureTask(callable, this.triggerTime(delay, unit), sequencer.getAndIncrement()));
      this.delayedExecute(t);
      return t;
    } else {
      throw new NullPointerException();
    }
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    if (command != null && unit != null) {
      if (period <= 0L) {
        throw new IllegalArgumentException();
      } else {
        ScheduledThreadPoolExecutor.ScheduledFutureTask<Void> sft = new ScheduledThreadPoolExecutor.ScheduledFutureTask(command, (Object)null, this.triggerTime(initialDelay, unit), unit.toNanos(period), sequencer.getAndIncrement());
        RunnableScheduledFuture<Void> t = this.decorateTask((Runnable)command, sft);
        sft.outerTask = t;
        this.delayedExecute(t);
        return t;
      }
    } else {
      throw new NullPointerException();
    }
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    if (command != null && unit != null) {
      if (delay <= 0L) {
        throw new IllegalArgumentException();
      } else {
        ScheduledThreadPoolExecutor.ScheduledFutureTask<Void> sft = new ScheduledThreadPoolExecutor.ScheduledFutureTask(command, (Object)null, this.triggerTime(initialDelay, unit), -unit.toNanos(delay), sequencer.getAndIncrement());
        RunnableScheduledFuture<Void> t = this.decorateTask((Runnable)command, sft);
        sft.outerTask = t;
        this.delayedExecute(t);
        return t;
      }
    } else {
      throw new NullPointerException();
    }
  }

  public void execute(Runnable command) {
    this.schedule(command, 0L, TimeUnit.NANOSECONDS);
  }

  public Future<?> submit(Runnable task) {
    return this.schedule(task, 0L, TimeUnit.NANOSECONDS);
  }

  public <T> Future<T> submit(Runnable task, T result) {
    return this.schedule(Executors.callable(task, result), 0L, TimeUnit.NANOSECONDS);
  }

  public <T> Future<T> submit(Callable<T> task) {
    return this.schedule(task, 0L, TimeUnit.NANOSECONDS);
  }

  public void setContinueExistingPeriodicTasksAfterShutdownPolicy(boolean value) {
    this.continueExistingPeriodicTasksAfterShutdown = value;
    if (!value && this.isShutdown()) {
      this.onShutdown();
    }

  }

  public boolean getContinueExistingPeriodicTasksAfterShutdownPolicy() {
    return this.continueExistingPeriodicTasksAfterShutdown;
  }

  public void setExecuteExistingDelayedTasksAfterShutdownPolicy(boolean value) {
    this.executeExistingDelayedTasksAfterShutdown = value;
    if (!value && this.isShutdown()) {
      this.onShutdown();
    }

  }

  public boolean getExecuteExistingDelayedTasksAfterShutdownPolicy() {
    return this.executeExistingDelayedTasksAfterShutdown;
  }

  public void setRemoveOnCancelPolicy(boolean value) {
    this.removeOnCancel = value;
  }

  public boolean getRemoveOnCancelPolicy() {
    return this.removeOnCancel;
  }

  public void shutdown() {
    super.shutdown();
  }

  public List<Runnable> shutdownNow() {
    return super.shutdownNow();
  }

  public BlockingQueue<Runnable> getQueue() {
    return super.getQueue();
  }

  static class DelayedWorkQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {
    private static final int INITIAL_CAPACITY = 16;
    private RunnableScheduledFuture<?>[] queue = new RunnableScheduledFuture[16];
    private final ReentrantLock lock = new ReentrantLock();
    private int size;
    private Thread leader;
    private final Condition available;

    DelayedWorkQueue() {
      this.available = this.lock.newCondition();
    }

    private static void setIndex(RunnableScheduledFuture<?> f, int idx) {
      if (f instanceof ScheduledThreadPoolExecutor.ScheduledFutureTask) {
        ((ScheduledThreadPoolExecutor.ScheduledFutureTask)f).heapIndex = idx;
      }

    }

    private void siftUp(int k, RunnableScheduledFuture<?> key) {
      while(true) {
        if (k > 0) {
          int parent = k - 1 >>> 1;
          RunnableScheduledFuture<?> e = this.queue[parent];
          if (key.compareTo(e) < 0) {
            this.queue[k] = e;
            setIndex(e, k);
            k = parent;
            continue;
          }
        }

        this.queue[k] = key;
        setIndex(key, k);
        return;
      }
    }

    private void siftDown(int k, RunnableScheduledFuture<?> key) {
      int child;
      for(int half = this.size >>> 1; k < half; k = child) {
        child = (k << 1) + 1;
        RunnableScheduledFuture<?> c = this.queue[child];
        int right = child + 1;
        if (right < this.size && c.compareTo(this.queue[right]) > 0) {
          child = right;
          c = this.queue[right];
        }

        if (key.compareTo(c) <= 0) {
          break;
        }

        this.queue[k] = c;
        setIndex(c, k);
      }

      this.queue[k] = key;
      setIndex(key, k);
    }

    private void grow() {
      int oldCapacity = this.queue.length;
      int newCapacity = oldCapacity + (oldCapacity >> 1);
      if (newCapacity < 0) {
        newCapacity = 2147483647;
      }

      this.queue = (RunnableScheduledFuture[])Arrays.copyOf(this.queue, newCapacity);
    }

    private int indexOf(Object x) {
      if (x != null) {
        int i;
        if (x instanceof ScheduledThreadPoolExecutor.ScheduledFutureTask) {
          i = ((ScheduledThreadPoolExecutor.ScheduledFutureTask)x).heapIndex;
          if (i >= 0 && i < this.size && this.queue[i] == x) {
            return i;
          }
        } else {
          for(i = 0; i < this.size; ++i) {
            if (x.equals(this.queue[i])) {
              return i;
            }
          }
        }
      }

      return -1;
    }

    public boolean contains(Object x) {
      ReentrantLock lock = this.lock;
      lock.lock();

      boolean var3;
      try {
        var3 = this.indexOf(x) != -1;
      } finally {
        lock.unlock();
      }

      return var3;
    }

    public boolean remove(Object x) {
      ReentrantLock lock = this.lock;
      lock.lock();

      boolean var6;
      try {
        int i = this.indexOf(x);
        if (i < 0) {
          boolean var10 = false;
          return var10;
        }

        setIndex(this.queue[i], -1);
        int s = --this.size;
        RunnableScheduledFuture<?> replacement = this.queue[s];
        this.queue[s] = null;
        if (s != i) {
          this.siftDown(i, replacement);
          if (this.queue[i] == replacement) {
            this.siftUp(i, replacement);
          }
        }

        var6 = true;
      } finally {
        lock.unlock();
      }

      return var6;
    }

    public int size() {
      ReentrantLock lock = this.lock;
      lock.lock();

      int var2;
      try {
        var2 = this.size;
      } finally {
        lock.unlock();
      }

      return var2;
    }

    public boolean isEmpty() {
      return this.size() == 0;
    }

    public int remainingCapacity() {
      return 2147483647;
    }

    public RunnableScheduledFuture<?> peek() {
      ReentrantLock lock = this.lock;
      lock.lock();

      RunnableScheduledFuture var2;
      try {
        var2 = this.queue[0];
      } finally {
        lock.unlock();
      }

      return var2;
    }

    public boolean offer(Runnable x) {
      if (x == null) {
        throw new NullPointerException();
      } else {
        RunnableScheduledFuture<?> e = (RunnableScheduledFuture)x;
        ReentrantLock lock = this.lock;
        lock.lock();

        try {
          int i = this.size;
          if (i >= this.queue.length) {
            this.grow();
          }

          this.size = i + 1;
          if (i == 0) {
            this.queue[0] = e;
            setIndex(e, 0);
          } else {
            this.siftUp(i, e);
          }

          if (this.queue[0] == e) {
            this.leader = null;
            this.available.signal();
          }
        } finally {
          lock.unlock();
        }

        return true;
      }
    }

    public void put(Runnable e) {
      this.offer(e);
    }

    public boolean add(Runnable e) {
      return this.offer(e);
    }

    public boolean offer(Runnable e, long timeout, TimeUnit unit) {
      return this.offer(e);
    }

    private RunnableScheduledFuture<?> finishPoll(RunnableScheduledFuture<?> f) {
      int s = --this.size;
      RunnableScheduledFuture<?> x = this.queue[s];
      this.queue[s] = null;
      if (s != 0) {
        this.siftDown(0, x);
      }

      setIndex(f, -1);
      return f;
    }

    public RunnableScheduledFuture<?> poll() {
      ReentrantLock lock = this.lock;
      lock.lock();

      RunnableScheduledFuture var3;
      try {
        RunnableScheduledFuture<?> first = this.queue[0];
        var3 = first != null && first.getDelay(TimeUnit.NANOSECONDS) <= 0L ? this.finishPoll(first) : null;
      } finally {
        lock.unlock();
      }

      return var3;
    }

    public RunnableScheduledFuture<?> take() throws InterruptedException {
      ReentrantLock lock = this.lock;
      lock.lockInterruptibly();

      try {
        while(true) {
          while(true) {
            RunnableScheduledFuture<?> first = this.queue[0];
            if (first != null) {
              long delay = first.getDelay(TimeUnit.NANOSECONDS);
              if (delay <= 0L) {
                RunnableScheduledFuture var14 = this.finishPoll(first);
                return var14;
              }

              first = null;
              if (this.leader != null) {
                this.available.await();
              } else {
                Thread thisThread = Thread.currentThread();
                this.leader = thisThread;

                try {
                  this.available.awaitNanos(delay);
                } finally {
                  if (this.leader == thisThread) {
                    this.leader = null;
                  }

                }
              }
            } else {
              this.available.await();
            }
          }
        }
      } finally {
        if (this.leader == null && this.queue[0] != null) {
          this.available.signal();
        }
        if (lock.isHeldByCurrentThread()) {
          lock.unlock();
        }
      }
    }

    public RunnableScheduledFuture<?> poll(long timeout, TimeUnit unit) throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      ReentrantLock lock = this.lock;
      lock.lockInterruptibly();

      try {
        while(true) {
          RunnableScheduledFuture<?> first = this.queue[0];
          if (first == null) {
            if (nanos <= 0L) {
              Object var22 = null;
              return (RunnableScheduledFuture)var22;
            }

            nanos = this.available.awaitNanos(nanos);
          } else {
            long delay = first.getDelay(TimeUnit.NANOSECONDS);
            if (delay <= 0L) {
              RunnableScheduledFuture var21 = this.finishPoll(first);
              return var21;
            }

            Thread thisThread;
            if (nanos <= 0L) {
              thisThread = null;
              return (RunnableScheduledFuture<?>) thisThread;
            }

            first = null;
            if (nanos >= delay && this.leader == null) {
              thisThread = Thread.currentThread();
              this.leader = thisThread;

              try {
                long timeLeft = this.available.awaitNanos(delay);
                nanos -= delay - timeLeft;
              } finally {
                if (this.leader == thisThread) {
                  this.leader = null;
                }

              }
            } else {
              nanos = this.available.awaitNanos(nanos);
            }
          }
        }
      } finally {
        if (this.leader == null && this.queue[0] != null) {
          this.available.signal();
        }
        if (lock.isHeldByCurrentThread()) {
          lock.unlock();
        }
      }
    }

    public void clear() {
      ReentrantLock lock = this.lock;
      lock.lock();

      try {
        for(int i = 0; i < this.size; ++i) {
          RunnableScheduledFuture<?> t = this.queue[i];
          if (t != null) {
            this.queue[i] = null;
            setIndex(t, -1);
          }
        }

        this.size = 0;
      } finally {
        lock.unlock();
      }

    }

    public int drainTo(Collection<? super Runnable> c) {
      return this.drainTo(c, 2147483647);
    }

    public int drainTo(Collection<? super Runnable> c, int maxElements) {
      Objects.requireNonNull(c);
      if (c == this) {
        throw new IllegalArgumentException();
      } else if (maxElements <= 0) {
        return 0;
      } else {
        ReentrantLock lock = this.lock;
        lock.lock();

        try {
          int n;
          RunnableScheduledFuture first;
          for(n = 0; n < maxElements && (first = this.queue[0]) != null && first.getDelay(TimeUnit.NANOSECONDS) <= 0L; ++n) {
            c.add(first);
            this.finishPoll(first);
          }

          int var9 = n;
          return var9;
        } finally {
          lock.unlock();
        }
      }
    }

    public Object[] toArray() {
      ReentrantLock lock = this.lock;
      lock.lock();

      Object[] var2;
      try {
        var2 = Arrays.copyOf(this.queue, this.size, Object[].class);
      } finally {
        lock.unlock();
      }

      return var2;
    }

    public <T> T[] toArray(T[] a) {
      ReentrantLock lock = this.lock;
      lock.lock();

      Object[] var3;
      try {
        if (a.length >= this.size) {
          System.arraycopy(this.queue, 0, a, 0, this.size);
          if (a.length > this.size) {
            a[this.size] = null;
          }

          var3 = a;
          return (T[]) var3;
        }

        var3 = Arrays.copyOf(this.queue, this.size, a.getClass());
      } finally {
        lock.unlock();
      }

      return (T[]) var3;
    }

    public Iterator<Runnable> iterator() {
      ReentrantLock lock = this.lock;
      lock.lock();

      ScheduledThreadPoolExecutor.DelayedWorkQueue.Itr var2;
      try {
        var2 = new ScheduledThreadPoolExecutor.DelayedWorkQueue.Itr((RunnableScheduledFuture[])Arrays.copyOf(this.queue, this.size));
      } finally {
        lock.unlock();
      }

      return var2;
    }

    private class Itr implements Iterator<Runnable> {
      final RunnableScheduledFuture<?>[] array;
      int cursor;
      int lastRet = -1;

      Itr(RunnableScheduledFuture<?>[] array) {
        this.array = array;
      }

      public boolean hasNext() {
        return this.cursor < this.array.length;
      }

      public Runnable next() {
        if (this.cursor >= this.array.length) {
          throw new NoSuchElementException();
        } else {
          return this.array[this.lastRet = this.cursor++];
        }
      }

      public void remove() {
        if (this.lastRet < 0) {
          throw new IllegalStateException();
        } else {
          DelayedWorkQueue.this.remove(this.array[this.lastRet]);
          this.lastRet = -1;
        }
      }
    }
  }

  private class ScheduledFutureTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {
    private final long sequenceNumber;
    private volatile long time;
    private final long period;
    RunnableScheduledFuture<V> outerTask = this;
    int heapIndex;

    ScheduledFutureTask(Runnable r, V result, long triggerTime, long sequenceNumber) {
      super(r, result);
      this.time = triggerTime;
      this.period = 0L;
      this.sequenceNumber = sequenceNumber;
    }

    ScheduledFutureTask(Runnable r, V result, long triggerTime, long period, long sequenceNumber) {
      super(r, result);
      this.time = triggerTime;
      this.period = period;
      this.sequenceNumber = sequenceNumber;
    }

    ScheduledFutureTask(Callable<V> callable, long triggerTime, long sequenceNumber) {
      super(callable);
      this.time = triggerTime;
      this.period = 0L;
      this.sequenceNumber = sequenceNumber;
    }

    public long getDelay(TimeUnit unit) {
      return unit.convert(this.time - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    public int compareTo(Delayed other) {
      if (other == this) {
        return 0;
      } else if (other instanceof ScheduledThreadPoolExecutor.ScheduledFutureTask) {
        ScheduledThreadPoolExecutor.ScheduledFutureTask<?> x = (ScheduledThreadPoolExecutor.ScheduledFutureTask)other;
        long diff = this.time - x.time;
        if (diff < 0L) {
          return -1;
        } else if (diff > 0L) {
          return 1;
        } else {
          return this.sequenceNumber < x.sequenceNumber ? -1 : 1;
        }
      } else {
        long diffx = this.getDelay(TimeUnit.NANOSECONDS) - other.getDelay(TimeUnit.NANOSECONDS);
        return diffx < 0L ? -1 : (diffx > 0L ? 1 : 0);
      }
    }

    public boolean isPeriodic() {
      return this.period != 0L;
    }

    private void setNextRunTime() {
      long p = this.period;
      if (p > 0L) {
        this.time += p;
      } else {
        this.time = ScheduledThreadPoolExecutor.this.triggerTime(-p);
      }

    }

    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled && ScheduledThreadPoolExecutor.this.removeOnCancel && this.heapIndex >= 0) {
        ScheduledThreadPoolExecutor.this.remove(this);
      }

      return cancelled;
    }

    public void run() {
      if (!ScheduledThreadPoolExecutor.this.canRunInCurrentRunState(this)) {
        this.cancel(false);
      } else if (!this.isPeriodic()) {
        super.run();
      } else if (super.runAndReset()) {
        this.setNextRunTime();
        ScheduledThreadPoolExecutor.this.reExecutePeriodic(this.outerTask);
      }

    }
  }
}
