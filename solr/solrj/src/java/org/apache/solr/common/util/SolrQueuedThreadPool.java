/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import org.apache.solr.common.ParWork;
import org.eclipse.jetty.util.AtomicBiInteger;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.eclipse.jetty.util.annotation.ManagedOperation;
import org.eclipse.jetty.util.annotation.Name;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.component.Dumpable;
import org.eclipse.jetty.util.component.DumpableCollection;
import org.eclipse.jetty.util.thread.ReservedThreadExecutor;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.util.thread.ThreadPoolBudget;
import org.eclipse.jetty.util.thread.TryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SolrQueuedThreadPool extends ContainerLifeCycle implements ThreadFactory, ThreadPool.SizedThreadPool, Dumpable, TryExecutor, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static Runnable NOOP = () ->
    {
    };

    /**
     * Encodes thread counts:
     * <dl>
     * <dt>Hi</dt><dd>Total thread count or Integer.MIN_VALUE if the pool is stopping</dd>
     * <dt>Lo</dt><dd>Net idle threads == idle threads - job queue size.  Essentially if positive,
     * this represents the effective number of idle threads, and if negative it represents the
     * demand for more threads</dd>
     * </dl>
     */
    private final AtomicBiInteger _counts = new AtomicBiInteger(Integer.MIN_VALUE, 0);
    private final AtomicLong _lastShrink = new AtomicLong();
    private final Set<Thread> _threads = ConcurrentHashMap.newKeySet();
    private final Object _joinLock = new Object();
    private final BlockingQueue<Runnable> _jobs;
    private final ThreadGroup _threadGroup;
    private final ThreadFactory _threadFactory;
    private String _name = "qtp" + hashCode();
    private int _idleTimeout;
    private int _maxThreads;
    private int _minThreads;
    private int _reservedThreads = -1;
    private TryExecutor _tryExecutor = TryExecutor.NO_TRY;
    private int _priority = Thread.NORM_PRIORITY;
    private boolean _daemon = false;
    private boolean _detailedDump = false;
    private int _lowThreadsThreshold = -1;
    private ThreadPoolBudget _budget;
    private volatile boolean closed;

    public SolrQueuedThreadPool() {
        this("solr-jetty-thread");
    }

    public SolrQueuedThreadPool(String name) {
        this(Integer.MAX_VALUE, 12,
                60000, 0, // no reserved executor threads - we can process requests after shutdown or some race - we try to limit without threadpool limits no anyway
                null, null,
                new  SolrNamedThreadFactory(name));
        this.name = name;
    }

    public SolrQueuedThreadPool(String name, int maxThreads, int minThreads, int idleTimeout) {
        this(maxThreads, minThreads,
            idleTimeout, -1,
            null, null,
            new  SolrNamedThreadFactory(name));
        this.name = name;
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads)
    {
        this(maxThreads, Math.min(8, maxThreads));
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads)
    {
        this(maxThreads, minThreads, 60000);
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads, @Name("queue") BlockingQueue<Runnable> queue)
    {
        this(maxThreads, minThreads, 60000, -1, queue, null);
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads, @Name("idleTimeout") int idleTimeout)
    {
        this(maxThreads, minThreads, idleTimeout, null);
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads, @Name("idleTimeout") int idleTimeout, @Name("queue") BlockingQueue<Runnable> queue)
    {
        this(maxThreads, minThreads, idleTimeout, queue, null);
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads, @Name("idleTimeout") int idleTimeout, @Name("queue") BlockingQueue<Runnable> queue, @Name("threadGroup") ThreadGroup threadGroup)
    {
        this(maxThreads, minThreads, idleTimeout, -1, queue, threadGroup);
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads,
                            @Name("idleTimeout") int idleTimeout, @Name("reservedThreads") int reservedThreads,
                            @Name("queue") BlockingQueue<Runnable> queue, @Name("threadGroup") ThreadGroup threadGroup)
    {
        this(maxThreads, minThreads, idleTimeout, reservedThreads, queue, threadGroup, null);
    }

    public SolrQueuedThreadPool(@Name("maxThreads") int maxThreads, @Name("minThreads") int minThreads,
                            @Name("idleTimeout") int idleTimeout, @Name("reservedThreads") int reservedThreads,
                            @Name("queue") BlockingQueue<Runnable> queue, @Name("threadGroup") ThreadGroup threadGroup,
                            @Name("threadFactory") ThreadFactory threadFactory)
    {
        if (maxThreads < minThreads)
            throw new IllegalArgumentException("max threads (" + maxThreads + ") less than min threads (" + minThreads + ")");
        setMinThreads(minThreads);
        setMaxThreads(maxThreads);
        setIdleTimeout(idleTimeout);
        setReservedThreads(reservedThreads);
        if (queue == null)
        {
            int capacity = Math.max(_minThreads, 8) * 1024;
            queue = new BlockingArrayQueue<>(capacity, capacity);
        }
        _jobs = queue;
        _threadGroup = threadGroup;
        setThreadPoolBudget(new ThreadPoolBudget(this));
        _threadFactory = threadFactory == null ? this : threadFactory;
    }

    @Override
    public ThreadPoolBudget getThreadPoolBudget()
    {
        return _budget;
    }

    public void setThreadPoolBudget(ThreadPoolBudget budget)
    {
        if (budget != null && budget.getSizedThreadPool() != this)
            throw new IllegalArgumentException();
        _budget = budget;
    }

    @Override
    protected void doStart() throws Exception
    {
        if (_reservedThreads == 0)
        {
            _tryExecutor = NO_TRY;
        }
        else
        {
            ReservedThreadExecutor reserved = new ReservedThreadExecutor(this, _reservedThreads);
            reserved.setIdleTimeout(_idleTimeout, TimeUnit.MILLISECONDS);
            _tryExecutor = reserved;
        }
        addBean(_tryExecutor);

        _lastShrink.set(System.nanoTime());

        super.doStart();
        // The threads count set to MIN_VALUE is used to signal to Runners that the pool is stopped.
        _counts.set(0, 0); // threads, idle
        if (!closed) {
            ensureThreads();
        }
    }

    @Override
    protected void doStop() throws Exception
    {
        if (LOG.isDebugEnabled())
            LOG.debug("Stopping {}", this);
        this.closed = true;
        this.setStopTimeout(0);
        super.doStop();

        removeBean(_tryExecutor);
        _tryExecutor = TryExecutor.NO_TRY;

        // Signal the Runner threads that we are stopping
        int threads = _counts.getAndSetHi(Integer.MIN_VALUE);

        // If stop timeout try to gracefully stop
        long timeout = getStopTimeout();
        BlockingQueue<Runnable> jobs = getQueue();
        if (timeout > 0)
        {
            // Fill the job queue with noop jobs to wakeup idle threads.
            for (int i = 0; i < threads; ++i)
            {
                jobs.offer(NOOP);
            }

            // try to let jobs complete naturally for half our stop time
            joinThreads(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout) / 2);

            // If we still have threads running, get a bit more aggressive

            // interrupt remaining threads
            for (Thread thread : _threads)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Interrupting {}", thread);
                thread.interrupt();
            }

            // wait again for the other half of our stop time
            joinThreads(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout) / 2);

            Thread.yield();
            if (LOG.isDebugEnabled())
            {
                for (Thread unstopped : _threads)
                {
                    StringBuilder dmp = new StringBuilder();
                    for (StackTraceElement element : unstopped.getStackTrace())
                    {
                        dmp.append(System.lineSeparator()).append("\tat ").append(element);
                    }
                    LOG.warn("Couldn't stop {}{}", unstopped, dmp.toString());
                }
            }
            else
            {
                for (Thread unstopped : _threads)
                {
                    LOG.warn("{} Couldn't stop {}", this, unstopped);
                }
            }
        }

        // Close any un-executed jobs
        while (!_jobs.isEmpty())
        {
            Runnable job = _jobs.poll();
            if (job instanceof Closeable)
            {
                try
                {
                    ((Closeable)job).close();
                }
                catch (Throwable t)
                {
                    LOG.warn("", t);
                }
            }
            else if (job != NOOP)
                LOG.warn("Stopped without executing or closing {}", job);
        }

        if (_budget != null)
            _budget.reset();

        synchronized (_joinLock)
        {
            _joinLock.notifyAll();
        }
    }

    private void joinThreads(long stopByNanos) throws InterruptedException
    {
        for (Thread thread : _threads)
        {
            long canWait = TimeUnit.NANOSECONDS.toMillis(stopByNanos - System.nanoTime());
            if (LOG.isDebugEnabled())
                LOG.debug("Waiting for {} for {}", thread, canWait);
            if (canWait > 0)
                thread.join(canWait);
        }
    }

    /**
     * Thread Pool should use Daemon Threading.
     *
     * @param daemon true to enable delegation
     * @see Thread#setDaemon(boolean)
     */
    public void setDaemon(boolean daemon)
    {
        _daemon = daemon;
    }

    /**
     * Set the maximum thread idle time.
     * Threads that are idle for longer than this period may be
     * stopped.
     *
     * @param idleTimeout Max idle time in ms.
     * @see #getIdleTimeout
     */
    public void setIdleTimeout(int idleTimeout)
    {
        _idleTimeout = idleTimeout;
        ReservedThreadExecutor reserved = getBean(ReservedThreadExecutor.class);
        if (reserved != null)
            reserved.setIdleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Set the maximum number of threads.
     *
     * @param maxThreads maximum number of threads.
     * @see #getMaxThreads
     */
    @Override
    public void setMaxThreads(int maxThreads)
    {
        if (_budget != null)
            _budget.check(maxThreads);
        _maxThreads = maxThreads;
        if (_minThreads > _maxThreads)
            _minThreads = _maxThreads;
    }

    /**
     * Set the minimum number of threads.
     *
     * @param minThreads minimum number of threads
     * @see #getMinThreads
     */
    @Override
    public void setMinThreads(int minThreads)
    {
        _minThreads = minThreads;

        if (_minThreads > _maxThreads)
            _maxThreads = _minThreads;

        if (isStarted())
            ensureThreads();
    }

    /**
     * Set the number of reserved threads.
     *
     * @param reservedThreads number of reserved threads or -1 for heuristically determined
     * @see #getReservedThreads
     */
    public void setReservedThreads(int reservedThreads)
    {
        if (isRunning())
            throw new IllegalStateException(getState());
        _reservedThreads = reservedThreads;
    }

    /**
     * @param name Name of this thread pool to use when naming threads.
     */
    public void setName(String name)
    {
        if (isRunning())
            throw new IllegalStateException("started");
        _name = name;
    }

    /**
     * Set the priority of the pool threads.
     *
     * @param priority the new thread priority.
     */
    public void setThreadsPriority(int priority)
    {
        _priority = priority;
    }

    /**
     * Get the maximum thread idle time.
     *
     * @return Max idle time in ms.
     * @see #setIdleTimeout
     */
    @ManagedAttribute("maximum time a thread may be idle in ms")
    public int getIdleTimeout()
    {
        return _idleTimeout;
    }

    /**
     * Get the maximum number of threads.
     *
     * @return maximum number of threads.
     * @see #setMaxThreads
     */
    @Override
    @ManagedAttribute("maximum number of threads in the pool")
    public int getMaxThreads()
    {
        return _maxThreads;
    }

    /**
     * Get the minimum number of threads.
     *
     * @return minimum number of threads.
     * @see #setMinThreads
     */
    @Override
    @ManagedAttribute("minimum number of threads in the pool")
    public int getMinThreads()
    {
        return _minThreads;
    }

    /**
     * Get the number of reserved threads.
     *
     * @return number of reserved threads or or -1 for heuristically determined
     * @see #setReservedThreads
     */
    @ManagedAttribute("the number of reserved threads in the pool")
    public int getReservedThreads()
    {
        if (isStarted())
        {
            ReservedThreadExecutor reservedThreadExecutor = getBean(ReservedThreadExecutor.class);
            if (reservedThreadExecutor != null)
                return reservedThreadExecutor.getCapacity();
        }
        return _reservedThreads;
    }

    /**
     * @return The name of the this thread pool
     */
    @ManagedAttribute("name of the thread pool")
    public String getName()
    {
        return _name;
    }

    /**
     * Get the priority of the pool threads.
     *
     * @return the priority of the pool threads.
     */
    @ManagedAttribute("priority of threads in the pool")
    public int getThreadsPriority()
    {
        return _priority;
    }

    /**
     * Get the size of the job queue.
     *
     * @return Number of jobs queued waiting for a thread
     */
    @ManagedAttribute("size of the job queue")
    public int getQueueSize()
    {
        // The idle counter encodes demand, which is the effective queue size
        int idle = _counts.getLo();
        return Math.max(0, -idle);
    }

    /**
     * @return whether this thread pool is using daemon threads
     * @see Thread#setDaemon(boolean)
     */
    @ManagedAttribute("thread pool uses daemon threads")
    public boolean isDaemon()
    {
        return _daemon;
    }

    @ManagedAttribute("reports additional details in the dump")
    public boolean isDetailedDump()
    {
        return _detailedDump;
    }

    public void setDetailedDump(boolean detailedDump)
    {
        _detailedDump = detailedDump;
    }

    @ManagedAttribute("threshold at which the pool is low on threads")
    public int getLowThreadsThreshold()
    {
        return _lowThreadsThreshold;
    }

    public void setLowThreadsThreshold(int lowThreadsThreshold)
    {
        _lowThreadsThreshold = lowThreadsThreshold;
    }

    @Override
    public void execute(Runnable job)
    {
        if (closed) {
            throw new RejectedExecutionException();
        }
        // Determine if we need to start a thread, use and idle thread or just queue this job
        int startThread;
        while (true)
        {
            // Get the atomic counts
            long counts = _counts.get();

            // Get the number of threads started (might not yet be running)
            int threads = AtomicBiInteger.getHi(counts);
            if (threads == Integer.MIN_VALUE)
                throw new RejectedExecutionException(job.toString());

            // Get the number of truly idle threads. This count is reduced by the
            // job queue size so that any threads that are idle but are about to take
            // a job from the queue are not counted.
            int idle = AtomicBiInteger.getLo(counts);

            // Start a thread if we have insufficient idle threads to meet demand
            // and we are not at max threads.
            startThread = (idle <= 0 && threads < _maxThreads) ? 1 : 0;

            // The job will be run by an idle thread when available
            if (!_counts.compareAndSet(counts, threads + startThread, idle + startThread - 1))
                continue;

            break;
        }

        if (!_jobs.offer(job))
        {
            // reverse our changes to _counts.
            if (addCounts(-startThread, 1 - startThread))
                LOG.warn("{} rejected {}", this, job);
            throw new RejectedExecutionException(job.toString());
        }

        if (LOG.isDebugEnabled())
            LOG.debug("queue {} startThread={}", job, startThread);

        // Start a thread if one was needed
        while (startThread-- > 0)
            startThread();
    }

    @Override
    public boolean tryExecute(Runnable task)
    {
        TryExecutor tryExecutor = _tryExecutor;
        return tryExecutor != null && tryExecutor.tryExecute(task);
    }

    @Override
    public void join() throws InterruptedException
    {
//        synchronized (_joinLock)
//        {
//            while (isRunning())
//            {
//                _joinLock.wait();
//            }
//        }
//
//        while (isStopping())
//        {
//            Thread.sleep(1);
//        }
    }

    /**
     * @return the total number of threads currently in the pool
     */
    @Override
    @ManagedAttribute("number of threads in the pool")
    public int getThreads()
    {
        int threads = _counts.getHi();
        return Math.max(0, threads);
    }

    /**
     * @return the number of idle threads in the pool
     */
    @Override
    @ManagedAttribute("number of idle threads in the pool")
    public int getIdleThreads()
    {
        int idle = _counts.getLo();
        return Math.max(0, idle);
    }

    /**
     * @return the number of busy threads in the pool
     */
    @ManagedAttribute("number of busy threads in the pool")
    public int getBusyThreads()
    {
        int reserved = _tryExecutor instanceof ReservedThreadExecutor ? ((ReservedThreadExecutor)_tryExecutor).getAvailable() : 0;
        return getThreads() - getIdleThreads() - reserved;
    }

    /**
     * <p>Returns whether this thread pool is low on threads.</p>
     * <p>The current formula is:</p>
     * <pre>
     * maxThreads - threads + idleThreads - queueSize &lt;= lowThreadsThreshold
     * </pre>
     *
     * @return whether the pool is low on threads
     * @see #getLowThreadsThreshold()
     */
    @Override
    @ManagedAttribute(value = "thread pool is low on threads", readonly = true)
    public boolean isLowOnThreads()
    {
        return getMaxThreads() - getThreads() + getIdleThreads() - getQueueSize() <= getLowThreadsThreshold();
    }

    private void ensureThreads()
    {
        while (true)
        {
            long counts = _counts.get();
            int threads = AtomicBiInteger.getHi(counts);
            if (threads == Integer.MIN_VALUE)
                break;

            // If we have less than min threads
            // OR insufficient idle threads to meet demand
            int idle = AtomicBiInteger.getLo(counts);
            if (threads < _minThreads || (idle < 0 && threads < _maxThreads))
            {
                // Then try to start a thread.
                if (_counts.compareAndSet(counts, threads + 1, idle + 1))
                    startThread();
                // Otherwise continue to check state again.
                continue;
            }
            break;
        }
    }

    protected void startThread()
    {
        boolean started = false;
        try
        {
            Thread thread = _threadFactory.newThread(_runnable);
            ParWork.getRootSharedExecutor().execute(thread);
            if (LOG.isDebugEnabled())
                LOG.debug("Starting {}", thread);
            _threads.add(thread);
            _lastShrink.set(System.nanoTime());
            _runnable.waitForStart();
            started = true;
        }
        finally
        {
            if (!started)
                addCounts(-1, -1); // threads, idle
        }
    }

    private boolean addCounts(int deltaThreads, int deltaIdle)
    {
        while (true)
        {
            long encoded = _counts.get();
            int threads = AtomicBiInteger.getHi(encoded);
            int idle = AtomicBiInteger.getLo(encoded);
            if (threads == Integer.MIN_VALUE) // This is a marker that the pool is stopped.
                return false;
            long update = AtomicBiInteger.encode(threads + deltaThreads, idle + deltaIdle);
            if (_counts.compareAndSet(encoded, update))
                return true;
        }
    }

    @Override
    public Thread newThread(Runnable runnable)
    {
        ThreadGroup group;

        {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ?
                s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        }
        Thread thread = new Thread(group, "") {
            @Override
            public void run() {
                try {
                    runnable.run();
                } finally {
                    ParWork.closeExecutor();
                }
            }
        };
        thread.setDaemon(isDaemon());
        thread.setPriority(getThreadsPriority());
        thread.setName(_name + "-" + thread.getId());
        return thread;
    }

    protected void removeThread(Thread thread)
    {
        _threads.remove(thread);
    }

    @Override
    public void dump(Appendable out, String indent) throws IOException
    {
        List<Object> threads = new ArrayList<>(getMaxThreads());
        for (final Thread thread : _threads)
        {
            final StackTraceElement[] trace = thread.getStackTrace();
            String knownMethod = "";
            for (StackTraceElement t : trace)
            {
                if ("idleJobPoll".equals(t.getMethodName()) && t.getClassName().equals(SolrQueuedThreadPool.Runner.class.getName()))
                {
                    knownMethod = "IDLE ";
                    break;
                }

                if ("reservedWait".equals(t.getMethodName()) && t.getClassName().endsWith("ReservedThread"))
                {
                    knownMethod = "RESERVED ";
                    break;
                }

                if ("select".equals(t.getMethodName()) && t.getClassName().endsWith("SelectorProducer"))
                {
                    knownMethod = "SELECTING ";
                    break;
                }

                if ("accept".equals(t.getMethodName()) && t.getClassName().contains("ServerConnector"))
                {
                    knownMethod = "ACCEPTING ";
                    break;
                }
            }
            final String known = knownMethod;

            if (isDetailedDump())
            {
                threads.add(new Dumpable()
                {
                    @Override
                    public void dump(Appendable out, String indent) throws IOException
                    {
                        if (StringUtil.isBlank(known))
                            Dumpable.dumpObjects(out, indent, String.format(Locale.ROOT,"%s %s %s %d", thread.getId(), thread.getName(), thread.getState(), thread.getPriority()), (Object[])trace);
                        else
                            Dumpable.dumpObjects(out, indent, String.format(Locale.ROOT,"%s %s %s %s %d", thread.getId(), thread.getName(), known, thread.getState(), thread.getPriority()));
                    }

                    @Override
                    public String dump()
                    {
                        return null;
                    }
                });
            }
            else
            {
                int p = thread.getPriority();
                threads.add(thread.getId() + " " + thread.getName() + " " + known + thread.getState() + " @ " + (trace.length > 0 ? trace[0] : "???") + (p == Thread.NORM_PRIORITY ? "" : (" prio=" + p)));
            }
        }

        if (isDetailedDump())
        {
            List<Runnable> jobs = new ArrayList<>(getQueue());
            dumpObjects(out, indent, new DumpableCollection("threads", threads), new DumpableCollection("jobs", jobs));
        }
        else
        {
            dumpObjects(out, indent, new DumpableCollection("threads", threads));
        }
    }

    @Override
    public String toString()
    {
        long count = _counts.get();
        int threads = Math.max(0, AtomicBiInteger.getHi(count));
        int idle = Math.max(0, AtomicBiInteger.getLo(count));
        int queue = getQueueSize();

        return String.format(Locale.ROOT, "%s[%s]@%x{%s,%d<=%d<=%d,i=%d,r=%d,q=%d}[%s]",
                getClass().getSimpleName(),
                _name,
                hashCode(),
                getState(),
                getMinThreads(),
                threads,
                getMaxThreads(),
                idle,
                getReservedThreads(),
                queue,
                _tryExecutor);
    }

    private final SolrQueuedThreadPool.Runner _runnable = new SolrQueuedThreadPool.Runner();

    /**
     * <p>Runs the given job in the {@link Thread#currentThread() current thread}.</p>
     * <p>Subclasses may override to perform pre/post actions before/after the job is run.</p>
     *
     * @param job the job to run
     */
    protected void runJob(Runnable job) {
        try {
            job.run();
        } catch (Error error) {
            log.error("Error in Jetty thread pool thread", error);
            this.error = error;
        } finally {
            ParWork.closeExecutor();
        }

        synchronized (notify) {
            notify.notifyAll();
        }
    }
    /**
     * @return the job queue
     */
    protected BlockingQueue<Runnable> getQueue()
    {
        return _jobs;
    }

    /**
     * @param queue the job queue
     * @deprecated pass the queue to the constructor instead
     */
    @Deprecated
    public void setQueue(BlockingQueue<Runnable> queue)
    {
        throw new UnsupportedOperationException("Use constructor injection");
    }

    /**
     * @param id the thread ID to interrupt.
     * @return true if the thread was found and interrupted.
     */
    @ManagedOperation("interrupts a pool thread")
    public boolean interruptThread(@Name("id") long id)
    {
        for (Thread thread : _threads)
        {
            if (thread.getId() == id)
            {
                thread.interrupt();
                return true;
            }
        }
        return false;
    }

    /**
     * @param id the thread ID to interrupt.
     * @return the stack frames dump
     */
    @ManagedOperation("dumps a pool thread stack")
    public String dumpThread(@Name("id") long id)
    {
        for (Thread thread : _threads)
        {
            if (thread.getId() == id)
            {
                StringBuilder buf = new StringBuilder();
                buf.append(thread.getId()).append(" ").append(thread.getName()).append(" ");
                buf.append(thread.getState()).append(":").append(System.lineSeparator());
                for (StackTraceElement element : thread.getStackTrace())
                {
                    buf.append("  at ").append(element.toString()).append(System.lineSeparator());
                }
                return buf.toString();
            }
        }
        return null;
    }

    private class Runner implements Runnable
    {
        CountDownLatch latch = new CountDownLatch(1);
        private Runnable idleJobPoll(long idleTimeout) throws InterruptedException
        {
            if (idleTimeout <= 0)
                return _jobs.take();
            return _jobs.poll(idleTimeout, TimeUnit.MILLISECONDS);
        }

        public void waitForStart() {
            try {
                latch.await();
            } catch (InterruptedException e) {

            }
        }

        @Override
        public void run()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Runner started for {}", SolrQueuedThreadPool.this);
            latch.countDown();
            boolean idle = true;
            try
            {
                Runnable job = null;
                while (true)
                {
                    // If we had a job,
                    if (job != null)
                    {
                        // signal that we are idle again
                        if (!addCounts(0, 1))
                            break;
                        idle = true;
                    }
                    // else check we are still running
                    else if (_counts.getHi() == Integer.MIN_VALUE)
                    {
                        break;
                    }

                    try
                    {
                        // Look for an immediately available job
                        job = _jobs.poll();
                        if (job == null)
                        {
                            // No job immediately available maybe we should shrink?
                            long idleTimeout = getIdleTimeout();
                            if (idleTimeout > 0 && getThreads() > _minThreads)
                            {
                                long last = _lastShrink.get();
                                long now = System.nanoTime();
                                if ((now - last) > TimeUnit.MILLISECONDS.toNanos(idleTimeout) && _lastShrink.compareAndSet(last, now))
                                {
                                    if (LOG.isDebugEnabled())
                                        LOG.debug("shrinking {}", SolrQueuedThreadPool.this);
                                    break;
                                }
                            }

                            // Wait for a job, only after we have checked if we should shrink
                            if (closed) {
                                job = _jobs.poll();
                            } else {
                                job = idleJobPoll(idleTimeout);
                            }


                            // If still no job?
                            if (job == null) {
                                if (closed) {
                                    break;
                                }
                                // continue to try again
                                continue;
                            }
                        }

                        idle = false;

                        // run job
                        if (LOG.isDebugEnabled())
                            LOG.debug("run {} in {}", job, SolrQueuedThreadPool.this);
                        runJob(job);
                        if (LOG.isDebugEnabled())
                            LOG.debug("ran {} in {}", job, SolrQueuedThreadPool.this);
                    }
                    catch (InterruptedException e)
                    {
                        if (LOG.isDebugEnabled())
                            LOG.debug("interrupted {} in {}", job, SolrQueuedThreadPool.this);
                    }
                    catch (Throwable e)
                    {
                        LOG.warn("", e);
                    }
                    finally
                    {
                        // Clear any interrupted status
                        Thread.interrupted();
                    }
                }
            }
            finally
            {
                Thread thread = Thread.currentThread();
                removeThread(thread);

                // Decrement the total thread count and the idle count if we had no job
                addCounts(-1, idle ? -1 : 0);
                if (LOG.isDebugEnabled())
                    LOG.debug("{} exited for {}", thread, SolrQueuedThreadPool.this);

                // There is a chance that we shrunk just as a job was queued for us, so
                // check again if we have sufficient threads to meet demand
                if (!closed) {
                    try {
                        ensureThreads();
                    } catch (RejectedExecutionException e) {
                        log.info("RejectedExecutionException encountered");
                    }
                }
            }
        }
    }


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private String name;
    private volatile Error error;
    private final Object notify = new Object();



    public void waitForStopping() throws InterruptedException {
        int threads = _counts.getAndSetHi(Integer.MIN_VALUE);
        BlockingQueue<Runnable> jobs = getQueue();
        // Fill the job queue with noop jobs to wakeup idle threads.
        for (int i = 0; i < threads; ++i)
        {
            jobs.offer(NOOP);
        }

        // try to let jobs complete naturally our stop time
        joinThreads( TimeUnit.MILLISECONDS.toNanos(getStopTimeout()));

    }

//    public void fillWithNoops() {
//        int threads = _counts.getAndSetHi(Integer.MIN_VALUE);
//        BlockingQueue<Runnable> jobs = getQueue();
//        // Fill the job queue with noop jobs to wakeup idle threads.
//        for (int i = 0; i < threads; ++i)
//        {
//            jobs.offer(NOOP);
//        }
//    }

    public void stopReserveExecutor() {
//        try {
//            ((ReservedThreadExecutor)_tryExecutor).stop();
//        } catch (Exception e) {
//            log.error("", e);
//        }
    }

    public void close() {
        try {
            if (!isStopping() || !isStopped()) {
                stop();
            }
            while (isStopping()) {
                Thread.sleep(10);
            }
        } catch (Exception e) {
            ParWork.propegateInterrupt("Exception closing", e);
            throw new RuntimeException(e);
        }

        assert ObjectReleaseTracker.release(this);
    }

//    @Override
//    public void doStop() throws Exception {
//      super.doStop();
//    }
//
//    public void stdStop() throws Exception {
//        super.doStop();
//    }

}