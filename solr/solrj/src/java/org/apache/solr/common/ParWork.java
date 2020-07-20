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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
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
  static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
  private static final String WORK_WAS_INTERRUPTED = "Work was interrupted!";

  private static final String RAN_INTO_AN_ERROR_WHILE_DOING_WORK =
      "Ran into an error while doing work!";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final static ThreadLocal<ExecutorService> THREAD_LOCAL_EXECUTOR = new ThreadLocal<>();

  private Set<Object> collectSet = null;

  private static SysStats sysStats = SysStats.getSysStats();

  private static class WorkUnit {
    private final List<Object> objects;
    private final TimeTracker tracker;
    private final String label;

    public WorkUnit(List<Object> objects, TimeTracker tracker, String label) {
      objects.remove(null);
      boolean ok = false;
      for (Object object : objects) {
        ok  = false;
        for (Class okobject : OK_CLASSES) {
          if (object == null || okobject.isAssignableFrom(object.getClass())) {
            ok = true;
            break;
          }
        }
        if (!ok) {
          throw new IllegalArgumentException(" -> I do not know how to close: " + object.getClass().getName());
        }
      }

      this.objects = objects;
      this.tracker = tracker;
      this.label = label;

      assert checkTypesForTests(objects);
    }

    private boolean checkTypesForTests(List<Object> objects) {
      for (Object object : objects) {
        assert !(object instanceof Collection);
        assert !(object instanceof Map);
        assert !(object.getClass().isArray());
      }

      return true;
    }
  }

  private static final Set<Class> OK_CLASSES = new HashSet<>();

  static {
    OK_CLASSES.add(ExecutorService.class);

    OK_CLASSES.add(OrderedExecutor.class);

    OK_CLASSES.add(Closeable.class);

    OK_CLASSES.add(AutoCloseable.class);

    OK_CLASSES.add(Callable.class);

    OK_CLASSES.add(Runnable.class);

    OK_CLASSES.add(Timer.class);

    OK_CLASSES.add(CloseableHttpClient.class);

  }

  private List<WorkUnit> workUnits = new CopyOnWriteArrayList();

  private final TimeTracker tracker;

  private final boolean ignoreExceptions;

  private Set<Throwable> warns = ParWork.concSetSmallO();

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
    this.ignoreExceptions = ignoreExceptions;
    tracker = new TimeTracker(object, object == null ? "NullObject" : object.getClass().getName());
    // constructor must stay very light weight
  }

  public void collect(Object object) {
    if (collectSet == null) {
      collectSet = new HashSet<>(64);
    }

    collectSet.add(object);
  }

  /**
   * @param callable A Callable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(Callable<?> callable) {
    if (collectSet == null) {
      collectSet = new HashSet<>();
    }
    collectSet.add(callable);
  }

  /**
   * @param runnable A Runnable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(Runnable runnable) {
    if (collectSet == null) {
      collectSet = new HashSet<>();
    }
    collectSet.add(runnable);
  }

  public void addCollect(String label) {
    if (collectSet == null) {
      log.info("No work collected to submit");
      return;
    }
    add(label, collectSet);
    collectSet.clear();
  }

  // add a unit of work
  public void add(String label, Object... objects) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object objects={}) - start", label, objects);
    }

    List<Object> objectList = new ArrayList<>(objects.length + 32);

    gatherObjects(objects, objectList);

    WorkUnit workUnit = new WorkUnit(objectList, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object) - end");
    }
  }

  public void add(Object object) {
    if (log.isDebugEnabled()) {
      log.debug("add(Object object={}) - start", object);
    }

    if (object == null)
      return;
    if (object instanceof Collection<?>) {
      throw new IllegalArgumentException("Use this method only with a single Object");
    }
    add(object.getClass().getName(), object);

    if (log.isDebugEnabled()) {
      log.debug("add(Object) - end");
    }
  }

  public void add(String label, Callable<?> callable) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, callable<?> callable={}) - start", label, callable);
    }
    WorkUnit workUnit = new WorkUnit(Collections.singletonList(callable), tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, callable<?>) - end");
    }
  }

  public void add(String label, Callable<?>... callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, callable<?> callables={}) - start", label, callables);
    }

    List<Object> objects = new ArrayList<>(callables.length);
    objects.addAll(Arrays.asList(callables));

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Callable<?>) - end");
    }
  }

  public void add(String label, Runnable... tasks) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Runnable tasks={}) - start", label, tasks);
    }

    List<Object> objects = new ArrayList<>(tasks.length);
    objects.addAll(Arrays.asList(tasks));

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Runnable) - end");
    }
  }
  public void add(String label, Runnable task) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Runnable tasks={}) - start", label, task);
    }

    List<Object> objects = new ArrayList<>(1);
    objects.add(task);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Runnable) - end");
    }
  }


  /**
   *
   * 
   * Runs the callable and closes the object in parallel. The object return will
   * be used for tracking. You can return a String if you prefer.
   * 
   */
  public void add(String label, Object object, Callable callable) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object={}, Callable Callable={}) - start", label, object, callable);
    }

    List<Object> objects = new ArrayList<>(2 + 32);
    objects.add(callable);

    gatherObjects(object, objects);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);

    if (log.isDebugEnabled()) {
      log.debug("add(String, Object, Callable) - end");
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void gatherObjects(Object object, List<Object> objects) {
    if (log.isDebugEnabled()) {
      log.debug("gatherObjects(Object object={}, List<Object> objects={}) - start", object, objects);
    }

    if (object != null) {
      if (object.getClass().isArray()) {
        if (log.isDebugEnabled()) {
          log.debug("Found an array to gather against");
        }

        for (Object obj : (Object[]) object) {
          gatherObjects(obj, objects);
        }

      } else if (object instanceof Collection) {
        if (log.isDebugEnabled()) {
          log.debug("Found a Collectiom to gather against");
        }
        for (Object obj : (Collection) object) {
          gatherObjects(obj, objects);
        }
      } else if (object instanceof Map<?, ?>) {
        if (log.isDebugEnabled()) {
          log.debug("Found a Map to gather against");
        }
        ((Map) object).forEach((k, v) -> gatherObjects(v, objects));
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Found a non collection object to add {}", object.getClass().getName());
        }
        objects.add(object);
      }
    }
  }

  public void add(String label, Object object, Callable... callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object={}, Callable Callables={}) - start", label, object, callables);
    }

    List<Object> objects = new ArrayList<>(callables.length + 1 + 32);
    objects.addAll(Arrays.asList(callables));
    gatherObjects(object, objects);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, Object object1, Object object2, Callable<?>... callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, Object object1={}, Object object2={}, Callable<?> callables={}) - start", label,
          object1, object2, callables);
    }

    List<Object> objects = new ArrayList<>(callables.length + 2 + 32);
    objects.addAll(Arrays.asList(callables));

    gatherObjects(object1, objects);
    gatherObjects(object2, objects);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
    if (log.isDebugEnabled()) {
      log.debug("Add WorkUnit:" + objects); // nocommit
    }
  }

  public void add(String label, Object object1, Object object2, Object object3, Callable<?>... callables) {
    if (log.isDebugEnabled()) {
      log.debug(
          "add(String label={}, Object object1={}, Object object2={}, Object object3={}, Callable<?> callables={}) - start",
          label, object1, object2, object3, callables);
    }

    List<Object> objects = new ArrayList<>(callables.length + 3 + 32);
    objects.addAll(Arrays.asList(callables));
    gatherObjects(object1, objects);
    gatherObjects(object2, objects);
    gatherObjects(object3, objects);

    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  public void add(String label, List<Callable<?>> callables) {
    if (log.isDebugEnabled()) {
      log.debug("add(String label={}, List<Callable<?>> callables={}) - start", label, callables);
    }

    List<Object> objects = new ArrayList<>(callables.size());
    objects.addAll(callables);
    WorkUnit workUnit = new WorkUnit(objects, tracker, label);
    workUnits.add(workUnit);
  }

  @Override
  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    if (collectSet != null && collectSet.size() > 1) {
      throw new IllegalStateException("addCollect must be called to add any objects collected!");
    }

    ExecutorService executor = getExecutor();
    //initExecutor();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    try {
      for (WorkUnit workUnit : workUnits) {
        //log.info("Process workunit {} {}", workUnit.label, workUnit.objects);
        final TimeTracker workUnitTracker = workUnit.tracker.startSubClose(workUnit.label);
        try {
          List<Object> objects = workUnit.objects;

          if (objects.size() == 1) {
            handleObject(workUnit.label, exception, workUnitTracker, objects.get(0));
          } else {

            List<Callable<Object>> closeCalls = new ArrayList<Callable<Object>>(objects.size());

            for (Object object : objects) {

              if (object == null)
                continue;

              closeCalls.add(() -> {
                handleObject(workUnit.label, exception, workUnitTracker, object);
                return object;
              });

            }
            if (closeCalls.size() > 0) {
              try {

                sizePoolByLoad();

                List<Future<Object>> results = executor.invokeAll(closeCalls, 8, TimeUnit.SECONDS);

                for (Future<Object> future : results) {
                  if (!future.isDone() || future.isCancelled()) {
                    log.warn("A task did not finish isDone={} isCanceled={}", future.isDone(), future.isCancelled());
                  //  throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "A task did nor finish" +future.isDone()  + " " + future.isCancelled());
                  }
                }

              } catch (InterruptedException e1) {
                log.warn(WORK_WAS_INTERRUPTED);
                Thread.currentThread().interrupt();
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

      exception.set(t);
    } finally {

      tracker.doneClose();
      
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

  public static void sizePoolByLoad() {
    Integer maxPoolsSize = getMaxPoolSize();

    ThreadPoolExecutor executor = (ThreadPoolExecutor) getExecutor();
    double load =  ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    if (load < 0) {
      log.warn("SystemLoadAverage not supported on this JVM");
      load = 0;
    }

    double ourLoad = sysStats.getAvarageUsagePerCPU();
    if (ourLoad > 1) {
      int cMax = executor.getMaximumPoolSize();
      if (cMax > 2) {
        executor.setMaximumPoolSize(Math.max(2, (int) ((double)cMax * 0.60D)));
      }
    } else {
      double sLoad = load / (double) PROC_COUNT;
      if (sLoad > 1.0D) {
        int cMax =  executor.getMaximumPoolSize();
        if (cMax > 2) {
          executor.setMaximumPoolSize(Math.max(2, (int) ((double) cMax * 0.60D)));
        }
      } else if (sLoad < 0.9D && maxPoolsSize != executor.getMaximumPoolSize()) {
        executor.setMaximumPoolSize(maxPoolsSize);
      }
      if (log.isDebugEnabled()) log.debug("ParWork, load:" + sLoad); //nocommit: remove when testing is done

    }
  }

  public static synchronized ExecutorService getExecutor() {
     // if (executor != null) return executor;
    ExecutorService exec = THREAD_LOCAL_EXECUTOR.get();
    if (exec == null) {
      if (log.isDebugEnabled()) {
        log.debug("Starting a new executor");
      }

      // figure out thread usage - maybe try to adjust based on current thread count
      exec = getExecutorService(0, 30, 5);
      THREAD_LOCAL_EXECUTOR.set(exec);
    }

    return exec;
  }

  public static ExecutorService getExecutorService(int corePoolSize, int maximumPoolSize, int keepAliveTime) {
    ThreadPoolExecutor exec;
    exec = new ParWorkExecutor("ParWork", getMaxPoolSize());
    return exec;
  }

  private static Integer getMaxPoolSize() {
    return Integer.getInteger("solr.maxThreadExecPoolSize",
            (int) Math.max(6, Math.round(Runtime.getRuntime().availableProcessors())));
  }

  private void handleObject(String label, AtomicReference<Throwable> exception, final TimeTracker workUnitTracker, Object object) {
    if (log.isDebugEnabled()) {
      log.debug(
          "handleObject(AtomicReference<Throwable> exception={}, CloseTimeTracker workUnitTracker={}, Object object={}) - start",
          exception, workUnitTracker, object);
    }

    if (object != null) {
      assert !(object instanceof Collection);
      assert !(object instanceof Map);
      assert !(object.getClass().isArray());
    }

    Object returnObject = null;
    TimeTracker subTracker = workUnitTracker.startSubClose(object);
    try {
      boolean handled = false;
      if (object instanceof ExecutorService) {
        shutdownAndAwaitTermination((ExecutorService) object);
        handled = true;
      } else if (object instanceof OrderedExecutor) {
        ((OrderedExecutor) object).shutdownAndAwaitTermination();
        handled = true;
      } else if (object instanceof OrderedExecutor) {
        ((OrderedExecutor) object).shutdownAndAwaitTermination();
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
        String msg = label + " -> I do not know how to close: " + object.getClass().getName();
        log.error(msg);
        IllegalArgumentException illegal = new IllegalArgumentException(msg);
        exception.set(illegal);
      }
    } catch (Throwable t) {

      if (t instanceof NullPointerException) {
        log.info("NPE closing " + object == null ? "Null Object" : object.getClass().getName());
      } else {
        if (ignoreExceptions) {
          warns.add(t);
          log.error("Error", t);
          if (t instanceof Error && !(t instanceof AssertionError)) {
            throw (Error) t;
          }
        } else {
          log.error("handleObject(AtomicReference<Throwable>=" + exception + ", CloseTimeTracker=" + workUnitTracker
              + ", Object=" + object + ")", t);
          propegateInterrupt(t);
          if (t instanceof Error) {
            throw (Error) t;
          }
          if (t instanceof  RuntimeException) {
            throw (RuntimeException) t;
          } else {
            throw new WorkException(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t); // TODO, hmm how do I keep zk session timeout and interrupt in play?
          }
        }
      }
    } finally {
      subTracker.doneClose(returnObject instanceof String ? (String) returnObject
          : (returnObject == null ? "" : returnObject.getClass().getName()));
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
    try (ParWork dw = new ParWork(object, ignoreExceptions)) {
      dw.add(object);
    }
  }

  public static void close(Object object) {
    try (ParWork dw = new ParWork(object)) {
      dw.add(object != null ? object.getClass().getSimpleName() : "null", object);
    }
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

  public static void propegateInterrupt(Throwable t) {
    propegateInterrupt(t, false);
  }

  public static void propegateInterrupt(Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.info("Interrupted", t.getMessage());
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(t.getMessage());
      } else {
        log.warn("Solr ran into an unexpected exception", t);
      }
    }

    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void propegateInterrupt(String msg, Throwable t) {
    propegateInterrupt(msg, t, false);
  }

  public static void propegateInterrupt(String msg, Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.info("Interrupted", t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(msg);
      } else {
        log.warn(msg, t);
      }
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if (pool == null)
      return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
    if (!(pool.isShutdown() && pool.isTerminated())) {
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

}
