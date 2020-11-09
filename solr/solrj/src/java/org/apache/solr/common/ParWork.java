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
package org.apache.solr.common;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.OrderedExecutor;
import org.apache.solr.common.util.SysStats;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParWork. A workhorse utility class that tries to use good patterns,
 * parallelism
 * 
 */
public class ParWork implements Closeable {
  public static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
  private static final String WORK_WAS_INTERRUPTED = "Work was interrupted!";

  private static final String RAN_INTO_AN_ERROR_WHILE_DOING_WORK =
      "Ran into an error while doing work!";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final static ThreadLocal<ExecutorService> THREAD_LOCAL_EXECUTOR = new ThreadLocal<>();
  private final boolean requireAnotherThread;
  private final String rootLabel;

  private final Set<ParObject> collectSet = ConcurrentHashMap.newKeySet(16);

  private static volatile ThreadPoolExecutor EXEC;

  // pretty much don't use it
  public static ThreadPoolExecutor getRootSharedExecutor() {
    if (EXEC == null) {
      synchronized (ParWork.class) {
        if (EXEC == null) {
          EXEC = (ThreadPoolExecutor) getParExecutorService("RootExec",
              Integer.getInteger("solr.rootSharedThreadPoolCoreSize", 250), Integer.MAX_VALUE, 5000,
              new SynchronousQueue());
          ((ParWorkExecutor)EXEC).enableCloseLock();
        }
      }
    }
    return EXEC;
  }

  public static void shutdownRootSharedExec() {
    shutdownRootSharedExec(true);
  }

  public static void shutdownRootSharedExec(boolean wait) {
    synchronized (ParWork.class) {
      if (EXEC != null) {
        ((ParWorkExecutor)EXEC).disableCloseLock();
        EXEC.shutdown();
        EXEC.setKeepAliveTime(1, TimeUnit.NANOSECONDS);
        EXEC.allowCoreThreadTimeOut(true);
       // EXEC.shutdownNow();
        if (wait) ExecutorUtil.shutdownAndAwaitTermination(EXEC);
        EXEC = null;
      }
    }
  }


  private static final SysStats sysStats = SysStats.getSysStats();

  public static SysStats getSysStats() {
    return sysStats;
  }

    private static class WorkUnit {
    private final Set<ParObject> objects;
    private final TimeTracker tracker;

    public WorkUnit(Set<ParObject> objects, TimeTracker tracker) {
      this.objects = objects;
      this.tracker = tracker;

    }
  }

  private static final Set<Class> OK_CLASSES;

  static {
    Set set = new HashSet<>(9);
    set.add(ExecutorService.class);
    set.add(OrderedExecutor.class);
    set.add(Closeable.class);
    set.add(AutoCloseable.class);
    set.add(Callable.class);
    set.add(Runnable.class);
    set.add(Timer.class);
    set.add(CloseableHttpClient.class);
    set.add(Map.class);
    OK_CLASSES = Collections.unmodifiableSet(set);
  }

  private final Queue<WorkUnit> workUnits = new ConcurrentLinkedQueue();

  private volatile TimeTracker tracker;

  private final boolean ignoreExceptions;

  private final Set<Throwable> warns = ParWork.concSetSmallO();

  // TODO should take logger as well
  public static class Exp extends Exception {

    private static final String ERROR_MSG = "Solr ran into an unexpected Exception";

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     */
    public Exp(String msg) {
      this(null, msg, null);
    }

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param th the exception to handle
     */
    public Exp(Throwable th) {
      this(null, th.getMessage(), th);
    }

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     * @param th  the exception to handle
     */
    public Exp(String msg, Throwable th) {
      this(null, msg, th);
    }

    public Exp(Logger classLog, String msg, Throwable th) {
      super(msg == null ? ERROR_MSG : msg, th);

      Logger logger;
      if (classLog != null) {
        logger = classLog;
      } else {
        logger = log;
      }

      logger.error(ERROR_MSG, th);
      if (th != null && th instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (th != null && th instanceof KeeperException) { // TODO maybe start using ZooKeeperException
        if (((KeeperException) th).code() == KeeperException.Code.SESSIONEXPIRED) {
          log.warn("The session has expired, give up any leadership roles!");
        }
      }
    }
  }

  public ParWork(Object object) {
    this(object, false);
  }


  public ParWork(Object object, boolean ignoreExceptions) {
    this(object, ignoreExceptions, false);
  }

  public ParWork(Object object, boolean ignoreExceptions, boolean requireAnotherThread) {
    this.ignoreExceptions = ignoreExceptions;
    this.requireAnotherThread = requireAnotherThread;
    this.rootLabel = object instanceof String ?
        (String) object : object.getClass().getSimpleName();
    assert (tracker = new TimeTracker(object, object == null ? "NullObject" : object.getClass().getName())) != null;
    // constructor must stay very light weight
  }

  public void collect(String label, Object object) {
    if (object == null) {
      return;
    }
    gatherObjects(label, object, collectSet);
  }

  public void collect(Object object) {
   collect(object != null ? object.toString() : null, object);
  }

  public void collect(Object... objects) {
    for (Object object : objects) {
      collect(object);
    }
  }

  /**
   * @param callable A Callable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(String label, Callable<?> callable) {
    collect(label, (Object) callable);
  }

  /**
   * @param runnable A Runnable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(String label, Runnable runnable) {
    collect(label, (Object) runnable);
  }

  public void addCollect() {
    if (collectSet.isEmpty()) {
      if (log.isDebugEnabled()) log.debug("No work collected to submit");
      return;
    }
    try {
      add(collectSet);
    } finally {
      collectSet.clear();
    }
  }

  private void gatherObjects(String label, Object submittedObject, Set<ParObject> collectSet) {
    if (submittedObject != null) {
      if (submittedObject instanceof Collection) {
        for (Object obj : (Collection) submittedObject) {
          ParObject ob = new ParObject();
          ob.object = obj;
          ob.label = label;
          collectSet.add(ob);
        }
      } else if (submittedObject instanceof Map) {
        ((Map) submittedObject).forEach((k, v) -> {
          ParObject ob = new ParObject();
          ob.object = v;
          ob.label = label;
          collectSet.add(ob);
        });
      } else {
        if (submittedObject instanceof ParObject) {
          collectSet.add((ParObject) submittedObject);
        } else {
          ParObject ob = new ParObject();
          ob.object = submittedObject;
          ob.label = label;
          collectSet.add(ob);
        }
      }
    }
  }

  private void add(Set<ParObject> objects) {
    if (log.isDebugEnabled()) {
      log.debug("add(String objects={}, objects");
    }



    Set<ParObject> wuObjects = new HashSet<>(objects.size());

    objects.forEach(parObject -> {

      verifyValidType(parObject);
      wuObjects.add(parObject);
    });

    WorkUnit workUnit = new WorkUnit(wuObjects, tracker);
    workUnits.add(workUnit);
  }

  private void verifyValidType(ParObject parObject) {
    Object object = parObject.object;

    boolean ok = false;
    for (Class okobject : OK_CLASSES) {
      if (okobject.isAssignableFrom(object.getClass())) {
        ok = true;
        break;
      }
    }
    if (!ok) {
      log.error(" -> I do not know how to close: " + object.getClass().getName());
      throw new IllegalArgumentException(" -> I do not know how to close: " + object.getClass().getName());
    }
  }

  @Override
  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    addCollect();

    boolean needExec = false;
    for (WorkUnit workUnit : workUnits) {
      if (workUnit.objects.size() > 1) {
        needExec = true;
      }
    }

    PerThreadExecService executor = null;
    if (needExec) {
      executor = (PerThreadExecService) getMyPerThreadExecutor();
    }
    //initExecutor();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    try {
      for (WorkUnit workUnit : workUnits) {
        if (log.isDebugEnabled()) log.debug("Process workunit {} {}", rootLabel, workUnit.objects);
        TimeTracker workUnitTracker = null;
        assert (workUnitTracker = workUnit.tracker.startSubClose(workUnit)) != null;
        try {
          Set<ParObject> objects = workUnit.objects;

          if (objects.size() == 1) {
            handleObject(exception, workUnitTracker, objects.iterator().next());
          } else {

            List<Callable<Object>> closeCalls = new ArrayList<>(objects.size());

            for (ParObject object : objects) {

              if (object == null)
                continue;

              TimeTracker finalWorkUnitTracker = workUnitTracker;
              if (requireAnotherThread) {
                closeCalls.add(new NoLimitsCallable<Object>() {
                  @Override
                  public Object call() {
                    try {
                      handleObject(exception, finalWorkUnitTracker,
                          object);
                    } catch (Throwable t) {
                      log.error(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t);
                      if (exception.get() == null) {
                        exception.set(t);
                      }
                    }
                    return object;
                  }
                });
              } else {
                closeCalls.add(() -> {
                  try {
                    handleObject(exception, finalWorkUnitTracker,
                        object);
                  } catch (Throwable t) {
                    log.error(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t);
                    if (exception.get() == null) {
                      exception.set(t);
                    }
                  }
                  return object;
                });
              }

            }
            if (closeCalls.size() > 0) {

                List<Future<Object>> results = new ArrayList<>(closeCalls.size());

                for (Callable call : closeCalls) {
                    Future future = executor.submit(call);
                    results.add(future);
                }

//                List<Future<Object>> results = executor.invokeAll(closeCalls, 8, TimeUnit.SECONDS);
              int i = 0;
                for (Future<Object> future : results) {
                  try {
                    future.get(
                        Long.getLong("solr.parwork.task_timeout", TimeUnit.MINUTES.toMillis(10)),
                        TimeUnit.MILLISECONDS); // nocommit
                    if (!future.isDone() || future.isCancelled()) {
                      log.warn("A task did not finish isDone={} isCanceled={}",
                          future.isDone(), future.isCancelled());
                      //  throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "A task did nor finish" +future.isDone()  + " " + future.isCancelled());
                    }
                  } catch (TimeoutException e) {
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout", e); // TODO: add object info eg ParObject.label
                  } catch (InterruptedException e1) {
                    log.warn(WORK_WAS_INTERRUPTED);
                    // TODO: save interrupted status and reset it at end?
                  }

                }
            }
          }
        } finally {
          if (workUnitTracker != null)
            workUnitTracker.doneClose();
        }

      }
    } catch (Throwable t) {
      log.error(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t);

      if (exception.get() == null) {
        exception.set(t);
      }
    } finally {

      assert tracker.doneClose();
      
      //System.out.println("DONE:" + tracker.getElapsedMS());

      warns.forEach((it) -> log.warn(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, new RuntimeException(it)));

      if (exception.get() != null) {
        Throwable exp = exception.get();
        if (exp instanceof Error) {
          throw (Error) exp;
        }
        if (exp instanceof  RuntimeException) {
          throw (RuntimeException) exp;
        }
        throw new RuntimeException(exp);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  public static ExecutorService getMyPerThreadExecutor() {
    Thread thread = Thread.currentThread();

    ExecutorService service = null;
    if (thread instanceof  SolrThread) {
      service = ((SolrThread) thread).getExecutorService();
    }

    if (service == null) {
      ExecutorService exec = THREAD_LOCAL_EXECUTOR.get();
      if (exec == null) {
        if (log.isDebugEnabled()) {
          log.debug("Starting a new executor");
        }

        Integer minThreads;
        Integer maxThreads;
        minThreads = 4;
        maxThreads = PROC_COUNT;
        exec = getExecutorService(Math.max(minThreads, maxThreads)); // keep alive directly affects how long a worker might
       // ((PerThreadExecService)exec).closeLock(true);
        // be stuck in poll without an enqueue on shutdown
        THREAD_LOCAL_EXECUTOR.set(exec);
      }
      service = exec;
    }

    return service;
  }

  public static ExecutorService getParExecutorService(String name, int corePoolSize, int maxPoolSize, int keepAliveTime, BlockingQueue queue) {
    ThreadPoolExecutor exec;
    exec = new ParWorkExecutor(name + "-" + Thread.currentThread().getName(),
            corePoolSize, maxPoolSize, keepAliveTime, queue);
    return exec;
  }

  public static ExecutorService getExecutorService(int maximumPoolSize) {
    return new PerThreadExecService(getRootSharedExecutor(), maximumPoolSize);
  }

  public static ExecutorService getExecutorService(int maximumPoolSize, boolean noCallerRunsAllowed, boolean noCallerRunsAvailableLimit) {
    return new PerThreadExecService(getRootSharedExecutor(), maximumPoolSize, noCallerRunsAllowed, noCallerRunsAvailableLimit);
  }

  private void handleObject(AtomicReference<Throwable> exception, final TimeTracker workUnitTracker, ParObject ob) {
    if (log.isDebugEnabled()) {
      log.debug(
          "handleObject(AtomicReference<Throwable> exception={}, CloseTimeTracker workUnitTracker={}, Object object={}) - start",
          exception, workUnitTracker, ob.object);
    }
    Object object = ob.object;

    Object returnObject = null;
    TimeTracker subTracker = null;
    assert (subTracker = workUnitTracker.startSubClose(object)) != null;
    try {
      boolean handled = false;
      if (object instanceof OrderedExecutor) {
        ((OrderedExecutor) object).shutdownAndAwaitTermination();
        handled = true;
      } else if (object instanceof ExecutorService) {
        shutdownAndAwaitTermination((ExecutorService) object);
        handled = true;
      } else if (object instanceof CloseableHttpClient) {
        HttpClientUtil.close((CloseableHttpClient) object);
        handled = true;
      } else if (object instanceof Closeable) {
        ((Closeable) object).close();
        handled = true;
      } else if (object instanceof AutoCloseable) {
        ((AutoCloseable) object).close();
        handled = true;
      } else if (object instanceof Callable) {
        returnObject = ((Callable<?>) object).call();
        handled = true;
      } else if (object instanceof Runnable) {
        ((Runnable) object).run();
        handled = true;
      } else if (object instanceof Timer) {
        ((Timer) object).cancel();
        handled = true;
      }

      if (!handled) {
        String msg = ob.label + " -> I do not know how to close " + ob.label  + ": " + object.getClass().getName();
        log.error(msg);
        IllegalArgumentException illegal = new IllegalArgumentException(msg);
        exception.set(illegal);
      }
    } catch (Throwable t) {
      if (ignoreExceptions) {
        warns.add(t);
        log.error("Error handling close for an object: " + ob.label + ": " + object.getClass().getSimpleName(), new ObjectReleaseTracker.ObjectTrackerException(t));
        if (t instanceof Error && !(t instanceof AssertionError)) {
          throw (Error) t;
        }
      } else {
        log.error("handleObject(AtomicReference<Throwable>=" + exception + ", CloseTimeTracker=" + workUnitTracker + ", Label=" + ob.label + ")" + ", Object=" + object + ")", t);
        propagateInterrupt(t);
        if (t instanceof Error) {
          throw (Error) t;
        }
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else {
          throw new WorkException(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t); // TODO, hmm how do I keep zk session timeout and interrupt in play?
        }
      }

    } finally {
      assert subTracker.doneClose(returnObject instanceof String ? (String) returnObject : (returnObject == null ? "" : returnObject.getClass().getName()));
    }

    if (log.isDebugEnabled()) {
      log.debug("handleObject(AtomicReference<Throwable>, CloseTimeTracker, List<Callable<Object>>, Object) - end");
    }
  }

  /**
   * Sugar method to close objects.
   * 
   * @param object to close
   */
  public static void close(Object object, boolean ignoreExceptions) {
    if (object == null) return;
    assert !(object instanceof ParObject);
    try (ParWork dw = new ParWork(object, ignoreExceptions)) {
      dw.collect(object);
    }
  }

  public static void close(Object object) {
    if (object == null) return;
    close(object, false);
  }

  public static <K> Set<K> concSetSmallO() {
    return ConcurrentHashMap.newKeySet(50);
  }

  public static <K, V> ConcurrentHashMap<K, V> concMapSmallO() {
    return new ConcurrentHashMap<K, V>(132, 0.75f, 50);
  }

  public static <K, V> ConcurrentHashMap<K, V> concMapReqsO() {
    return new ConcurrentHashMap<>(128, 0.75f, 2048);
  }

  public static <K, V> ConcurrentHashMap<K, V> concMapClassesO() {
    return new ConcurrentHashMap<>(132, 0.75f, 8192);
  }

  public static void propagateInterrupt(Throwable t) {
    propagateInterrupt(t, false);
  }

  public static void propagateInterrupt(Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.info("Interrupted", t.getMessage());
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        if (log.isDebugEnabled()) {
          log.info(t.getClass().getName() + " " + t.getMessage(), t);
        } else {
          log.info(t.getClass().getName() + " " + t.getMessage(), t);
        }
      } else {
        log.warn("Solr ran into an unexpected exception", t);
      }
    }

    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void propagateInterrupt(String msg, Throwable t) {
    propagateInterrupt(msg, t, false);
  }

  public static void propagateInterrupt(String msg, Throwable t, boolean infoLogMsg) {
    if (t != null && t instanceof InterruptedException) {
      log.info("Interrupted", t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(msg);
      } else {
        log.warn(msg, t);
      }
    }
    if (t != null && t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if (pool == null)
      return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
    if (!(pool.isShutdown())) {
      throw new RuntimeException("Timeout waiting for executor to shutdown");
    }

  }

  public static void awaitTermination(ExecutorService pool) {
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }

  public static abstract class NoLimitsCallable<V> implements Callable<Object> {
    @Override
    public abstract Object call() throws Exception;
  }

  public static class SolrFutureTask extends FutureTask implements SolrThread.CreateThread {

    private final boolean callerThreadAllowed;
    private final SolrThread createThread;

    public SolrFutureTask(Callable callable, boolean callerThreadAllowed) {
      super(callable);
      this.callerThreadAllowed = callerThreadAllowed;
      Thread thread = Thread.currentThread();
      if (thread instanceof  SolrThread) {
        this.createThread = (SolrThread) Thread.currentThread();
      } else {
        this.createThread = null;
      }
    }

    public SolrFutureTask(Runnable runnable, Object value) {
      this(runnable, value, true);
    }

    public SolrFutureTask(Runnable runnable, Object value, boolean callerThreadAllowed) {
      super(runnable, value);
      this.callerThreadAllowed = callerThreadAllowed;
      Thread thread = Thread.currentThread();
      if (thread instanceof  SolrThread) {
        this.createThread = (SolrThread) Thread.currentThread();
      } else {
        this.createThread = null;
      }
    }

    public boolean isCallerThreadAllowed() {
      return callerThreadAllowed;
    }

    @Override
    public SolrThread getCreateThread() {
      return createThread;
    }
  }

  private static class ParObject {
    String label;
    Object object;
  }
}
