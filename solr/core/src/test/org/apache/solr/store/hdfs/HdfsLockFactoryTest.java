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
package org.apache.solr.store.hdfs;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockLostException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.TestRuleRestoreSystemProperties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.solr.store.hdfs.HdfsLockFactory.DEFAULT_LOCK_HOLD_TIMEOUT;
import static org.apache.solr.store.hdfs.HdfsLockFactory.DEFAULT_UPDATE_DELAY;
import static org.apache.solr.store.hdfs.HdfsLockFactory.LOCK_HOLD_TIMEOUT_KEY;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;


@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsLockFactoryTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static MiniDFSCluster dfsCluster;
  private static NameNode nameNode;
  private static Configuration nameNodeConf;
  private long waitTime = 0;
  private List<ScheduledTaskStub> allTasks = new LinkedList<>();
  private HdfsDirectory dir;
  private Path lockPath;
  private HdfsLockFactory.HdfsLock lock = null;
  private HdfsLockFactory.HdfsLock lock2 = null;
  private Exception latestException = null;
  private List<HdfsLockFactory.HdfsLock> allLocks = new LinkedList<>();
  private static MiniDFSCluster.DataNodeProperties dataNodeProperties;

  @Rule
  public TestRuleRestoreSystemProperties p = new TestRuleRestoreSystemProperties(LOCK_HOLD_TIMEOUT_KEY);
  private LockLostException lockLostException;
  private List<ScheduledTaskStub> executing = new LinkedList<>();

  @BeforeClass
  public static void beforeClass() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    dfsCluster = HdfsTestUtil.setupClassBasic(createTempDir().toFile().getAbsolutePath());
    nameNode = dfsCluster.getNameNode();
    nameNodeConf = dfsCluster.getConfiguration(0);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
    nameNode = null;
    dataNodeProperties = null;
  }

  @Before
  public void setupHdfsLockFactory() {
    System.setProperty(LOCK_HOLD_TIMEOUT_KEY, String.valueOf(DEFAULT_LOCK_HOLD_TIMEOUT));
    Consumer<Exception> exceptionHandler = e -> {
      latestException = e;
      if (e instanceof LockLostException)
        lockLostException = (LockLostException) e;
    };
    HdfsLockFactory.INSTANCE.setExceptionHandler(exceptionHandler);
    ScheduledExecutorService exec = mock(ScheduledExecutorService.class);
    given(exec.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
        .willAnswer(invocation -> {
          Object[] a = invocation.getArguments();
          ScheduledTaskStub task = new ScheduledTaskStub((Runnable) a[0], (Long) a[1], (Long) a[2], (TimeUnit) a[3]);
          allTasks.add(task);
          return task;
        });
    HdfsLockFactory.INSTANCE.setExecutorService(exec);
  }

  @AfterClass
  public static void resetTimeDependencies() throws Exception {
    HdfsLockFactory.INSTANCE.reset();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    startStoppedDatanode();
    String uri = HdfsTestUtil.getURI(dfsCluster);
    lockPath = new Path(uri, "/basedir/lock");
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    dir = new HdfsDirectory(lockPath, conf);
    HdfsLockFactory.INSTANCE.setSleeper(m -> {
      waitTime += m;
      executeScheduledTasks();
    });
  }

  private void startStoppedDatanode() throws IOException, InterruptedException {
    if (dataNodeProperties != null) {
      dfsCluster.restartDataNode(dataNodeProperties, true);
      for (int i = 0; i < 1200; i++) {
        if (dfsCluster.getDataNodes().get(0).isDatanodeFullyStarted()) break;
        Thread.sleep(100);
      }
      assertTrue(dfsCluster.getDataNodes().get(0).isDatanodeFullyStarted());
      dataNodeProperties = null;
    }
  }

  @After
  public void releaseLocks() throws Exception {
    allLocks.forEach(l -> {
      try {
        l.close();
      } catch (IOException ignored) {
      }
    });
    startStoppedDatanode();
    DistributedFileSystem fs = dfsCluster.getFileSystem();
    List<FileStatus> lockFiles = asList(fs.listStatus(lockPath));
    lockFiles.forEach(s -> {
      try {
        fs.delete(s.getPath(), true);
      } catch (IOException ignored) {
      }
    });
  }

  @Test
  public void testBasic() throws IOException {
    String uri = HdfsTestUtil.getURI(dfsCluster);
    Path lockPath = new Path(uri, "/basedir/lock");
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    HdfsDirectory dir = new HdfsDirectory(lockPath, conf);


    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    // now repeat after close()
    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    dir.close();
  }

  @Test
  public void testBasic_SecondLockFails() throws IOException {
    lock = obtainLock();
    try {
      lock2 = obtainLock();
      fail();
    } catch (LockObtainFailedException ignored) {
    }
  }

  @Test
  public void testBasic_LockCanBeRepeated() throws IOException {
    lock = obtainLock();
    failObtainingLock();
    lock.close();
    lock = obtainLock();
    failObtainingLock();
  }

  @Test
  public void lockIsNotLostIfCannotUpdateForAWhile() throws Exception {
    System.setProperty(LOCK_HOLD_TIMEOUT_KEY, "0");
    lock = obtainLock();
    disconnectFromHdfs(lock);
    executeScheduledTasks();
    assertFalse(lockLostReported());
  }

  @Test
  public void lockCanBeTakenOverIfNotRefreshed() throws IOException {
    lock = obtainLock();
    disconnectFromHdfs(lock);
    lock2 = obtainLock();

    assertTrue("waitTime should be greater than or equal to 5000", waitTime >= 5000L);
  }

  @Test
  public void lockFailsEarlyDueToPollingItsStatus() throws IOException {
    lock = obtainLock();
    disconnectFromHdfs(lock);
    HdfsLockFactory.INSTANCE.setSleeper(t -> {
      this.waitTime += t;
      if (waitTime >= 3 * DEFAULT_UPDATE_DELAY)
        reconnectHdfs(lock);
      executeScheduledTasks();
    });
    failObtainingLock();
    assertEquals(DEFAULT_UPDATE_DELAY * 3, waitTime);
  }

  @Test
  public void lockIsLostIfIdIsOverWrittenInIt() throws IOException {
    lock = obtainLock();
    disconnectFromHdfs(lock);
    lock2 = obtainLock();
    reconnectHdfs(lock);
    executeScheduledTasks();
    assertTrue(lockLostReported());
  }

  @Test
  public void willNotReportLockLostIfDisconnected() throws Exception {
    lock = obtainLock();
    for (int i = 0; i < 20; i++) {
      executeScheduledTasks();
    }
    assertFalse(lockLostReported());
  }

  @Test
  public void only1outOf2getsTheLock() throws Exception {
    lock = obtainLock();
    disconnectFromHdfs(lock);
    CountDownLatch countDown = new CountDownLatch(2);
    final AtomicInteger successful = new AtomicInteger(0);
    new Thread(() -> {
      try {
        obtainLock();
        successful.incrementAndGet();
      } catch (Exception ignored) {

      } finally {
        countDown.countDown();
      }
    }).run();
    new Thread(() -> {
      try {
        obtainLock();
        successful.incrementAndGet();
      } catch (Exception ignored) {

      } finally {
        countDown.countDown();
      }
    }).run();
    countDown.await(10_000, TimeUnit.MILLISECONDS);
    assertEquals(1, successful.get());
  }

  private boolean lockLostReported() {
    return lockLostException != null;
  }

  private void failObtainingLock() throws IOException {
    try {
      obtainLock();
      fail();
    } catch (LockObtainFailedException ignored) {
    }
  }

  private void reconnectHdfs(HdfsLockFactory.HdfsLock lock) {
    try {
      setFileSystem(lock, dfsCluster.getFileSystem());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private HdfsLockFactory.HdfsLock obtainLock() throws IOException {
    HdfsLockFactory.HdfsLock lock = (HdfsLockFactory.HdfsLock) dir.obtainLock("testlock");
    allLocks.add(lock);
    return lock;
  }

  private void disconnectFromHdfs(HdfsLockFactory.HdfsLock lock) {
    DistributedFileSystem failing = mock(DistributedFileSystem.class, i -> {
      throw new ConnectException("Injected failure");
    });
    setFileSystem(lock, failing);
  }

  private void setFileSystem(HdfsLockFactory.HdfsLock lock, DistributedFileSystem failing) {
    try {
      FieldSetter.setField(lock, HdfsLockFactory.HdfsLock.class.getDeclaredField("fs"), failing);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private class ScheduledTaskStub<T> implements ScheduledFuture<T> {
    Runnable task;
    long inititalDelay;
    long delay;
    TimeUnit unit;
    private boolean done = false;

    private ScheduledTaskStub(Runnable task, long inititalDelay, long delay, TimeUnit unit) {
      this.task = task;
      this.inititalDelay = inititalDelay;
      this.delay = delay;
      this.unit = unit;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return delay;
    }

    @Override
    public int compareTo(Delayed o) {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return allTasks.remove(ScheduledTaskStub.this);
    }

    @Override
    public boolean isCancelled() {
      return allTasks.contains(ScheduledTaskStub.this);
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return null;
    }

  }

  private void executeScheduledTasks() {
    LinkedList<ScheduledTaskStub> waitList = new LinkedList<>(allTasks);
    waitList.removeAll(executing);
    waitList.forEach(sch -> {
      try {
        executing.add(sch);
        sch.task.run();
      } catch (RuntimeException ignored) {
      } finally {
        executing.remove(sch);
      }
    });
  }

}
