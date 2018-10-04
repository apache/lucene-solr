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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockLostException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockReleaseFailedException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsLockFactory extends LockFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final long DEFAULT_LOCK_ACQUIRE_TIMEOUT = 90_000L;
  public static final long DEFAULT_LOCK_HOLD_TIMEOUT = DEFAULT_LOCK_ACQUIRE_TIMEOUT / 2;
  public static final long DEFAULT_UPDATE_DELAY = 5000L;
  public static final HdfsLockFactory INSTANCE = new HdfsLockFactory();
  public static final String HDFS_LOCK_UPDATE_PREFIX = "hdfs-lock-update";
  public static final int HDFS_LOCK_UPDATE_THREADS = 5;

  public static final String LOCK_HOLD_TIMEOUT_KEY = "hdfs.lock.update.hold.timeout";
  public static final String UPDATE_DELAY_KEY = "hdfs.lock.update.delay";
  public static final String HDFS_LOCK_ACQUIRE_TIMEOUT_KEY = "hdfs.lock.acquire.timeout";
  private static final int MAX_META_LOCK_DEPTH = 10;

  private SleepService sleeper;

  private volatile ScheduledExecutorService executor;
  private Consumer<Exception> exceptionHandler;
  private final Object executorGuard = new Object();

  private HdfsLockFactory() {
    reset();
  }

  void reset() {
    sleeper = m -> {
      try {
        Thread.sleep(m);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
    executor = null;
  }

  public void stopBackgroundTasks() {
    if(executor == null)
      return;
    try {
      ScheduledExecutorService executorToShutDown;
      synchronized (executorGuard) {
        executorToShutDown = executor;
        executor = null;
      }
      executorToShutDown.shutdownNow();
      executorToShutDown.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ScheduledExecutorService newExecutor() {
    return Executors.newScheduledThreadPool(HDFS_LOCK_UPDATE_THREADS, new DefaultSolrThreadFactory(HDFS_LOCK_UPDATE_PREFIX));
  }

  private ScheduledExecutorService getExecutor() {
    if(executor != null)
      return executor;
    synchronized (executorGuard) {
      if (executor == null)
        executor = newExecutor();
      return executor;
    }
  }


  public void setExecutorService(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public void setSleeper(SleepService sleeper) {
    this.sleeper = sleeper;
  }

  public void setExceptionHandler(Consumer<Exception> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
  }


  public interface SleepService {
    public void sleep(long millis);
  }

  @Override
  public Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (!(dir instanceof HdfsDirectory)) {
      throw new UnsupportedOperationException("HdfsLockFactory can only be used with HdfsDirectory subclasses, got: " + dir);
    }
    final HdfsDirectory hdfsDir = (HdfsDirectory) dir;
    final Configuration conf = hdfsDir.getConfiguration();
    final Path lockPath = hdfsDir.getHdfsDirPath();
    final Path lockFile = new Path(lockPath, lockName);

    final FileSystem fs = FileSystem.get(lockPath.toUri(), conf);
    log.info("Trying to lock on " + fs.getClass().getName() + " the following path:" + lockPath);
    HdfsLock hdfsLock;
    createDirs(lockPath, fs);
    while (true) {
      try {
        log.info("Trying to create directory {}, exists: {}", lockPath.getName(), fs.exists(lockFile));
        hdfsLock = new HdfsLock(conf, lockFile, fs);
        hdfsLock.init();
        break;
      } catch (SafeModeException e) {
        log.warn("The NameNode is in SafeMode - Solr will wait 5 seconds and try again.");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
          Thread.interrupted();
        }
      } catch (IOException e) {
        throw new LockObtainFailedException("Cannot obtain lock file: " + lockFile, e);
      }
    }
    hdfsLock.startScheduledUpdate();
    return hdfsLock;
  }

  private int getLockDepth(Path lockFile, FileSystem fs) throws IOException {
    Path l = lockFile;
    int i;
    for (i = 0; i < MAX_META_LOCK_DEPTH;i++) {
      if (!fs.exists(l)) break;
      l = getMetaLock(l);
    }
    return i;
  }

  private void createDirs(Path lockPath, FileSystem fs) throws IOException {
    if (!fs.exists(lockPath)) {
      boolean success = fs.mkdirs(lockPath);
      if (!success) {
        throw new RuntimeException("Could not create directory: " + lockPath);
      }
    } else {
      // just to check for safe mode
      fs.mkdirs(lockPath);
    }
  }

  private Path getMetaLock(Path lockFile, int lockDepth) {
    Path monitor = lockFile;
    for(int i= 1; i<lockDepth; i++)
      monitor = getMetaLock(lockFile);
    return monitor;
  }

  private void removeLock(Path lockFile, FileSystem fs) throws IOException {
    fs.delete(lockFile, false);
  }

  private Path getMetaLock(Path lockFile) {
    return new Path(lockFile.getParent(), lockFile.getName() + ".lock");
  }

  private class LockInfo {
    private UUID id;
    private long version;

    private LockInfo(UUID id, long version) {
      this.id = id;
      this.version = version;
    }

    @Override
    public String toString() {
      return ReflectionToStringBuilder.reflectionToString(this);
    }

    @Override
    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return HashCodeBuilder.reflectionHashCode(this);
    }
  }

  private LockInfo readLockInfo(Path lockFile, FileSystem fs) throws IOException {
    try (FSDataInputStream lockStream = fs.open(lockFile)) {
      return new LockInfo(UUID.fromString(lockStream.readUTF()), lockStream.readLong());
    }
  }


  public final class HdfsLock extends Lock {
    private final UUID id = UUID.randomUUID();
    private final Configuration conf;
    private final Path lockFile;
    private final long updateDelay;
    private final long updateTimeoutHold;
    private FileSystem fs;
    private volatile boolean closed;
    private ScheduledFuture<?> scheduledFileUpdate;
    private long cycle = 0;
    long lastLockUpdate = 0;
    private AtomicReference<Optional<Path>> deleteGuard = new AtomicReference(Optional.empty());
    private boolean lockExpired = false;

    HdfsLock(Configuration conf, Path lockFile, FileSystem fs) {
      this.conf = conf;
      this.lockFile = lockFile;
      this.fs = fs;
      updateDelay = getUpdateDelay();
      updateTimeoutHold = Long.valueOf(System.getProperty(LOCK_HOLD_TIMEOUT_KEY, String.valueOf(DEFAULT_LOCK_HOLD_TIMEOUT)));
    }
    
    @Override
    public void close() throws IOException {
      final FileSystem fs = FileSystem.get(lockFile.toUri(), conf);
      if (closed) {
        return;
      }
      closed = true;
      stopUpdates();
      deleteGuard();
      try {
        if (fs.exists(lockFile) && !fs.delete(lockFile, false)) {
          throw new LockReleaseFailedException("failed to delete: " + lockFile);
        }
      } finally {
        IOUtils.closeQuietly(fs);
      }
    }

    @Override
    public void ensureValid() throws IOException {
      UUID actualId = readLockInfo(lockFile, fs).id;
      if ( !actualId.equals(id) )
        throw new LockLostException("Another process took ownership of the lock. Lock id: " + actualId + " own id: " + id);
    }

    @Override
    public String toString() {
      return "HdfsLock(lockFile=" + lockFile + ")";
    }

    void stopUpdates() {
      scheduledFileUpdate.cancel(true);
    }

    void startScheduledUpdate() {
      scheduledFileUpdate = getExecutor().scheduleWithFixedDelay(() -> {
        try {
          if(lockExpired)
            reAcquireLock();
          else {
            ensureValid();
            createLockFile(true);
          }
        } catch (IOException | RuntimeException e) {
          if (System.nanoTime() - lastLockUpdate  >= updateTimeoutHold * 1_000_000) {
            lockExpired = true;
          }
          exceptionHandler.accept(e);
          throw new RuntimeException(e);
        }
      }, updateDelay, updateDelay, TimeUnit.MILLISECONDS);
    }

    private void reAcquireLock() {
      try {
        int i = getLockDepth(lockFile, fs);
        if(i > 0) {
          long lockTimeout = Long.valueOf(System.getProperty(HDFS_LOCK_ACQUIRE_TIMEOUT_KEY, String.valueOf(DEFAULT_LOCK_ACQUIRE_TIMEOUT)));
          Path monitor = getMetaLock(lockFile, i);
          LockInfo ownLockInfo = new LockInfo(id, cycle - 1);
          LockInfo actual = expectUnchangedLockInfo(monitor, ownLockInfo, lockTimeout);
          if(ownLockInfo.equals(actual)) {
            removeStaleLock(lockFile, i);
            createLockFile(false);
            lockExpired = false;
          } else {
            exceptionHandler.accept(new LockLostException("Another process took ownership of the lock. Lock id: " + actual.id + " own id: " + id));
            stopUpdates();
          }
        }

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void init() throws IOException {
      int i = getLockDepth(lockFile, fs);
      if(i > 0) {
        removeIfUntouched(lockFile, i);
      }
      createLockFile(false);

    }

    private void removeIfUntouched(Path lockFile, int lockDepth) throws IOException {
      long lockTimeout = Long.valueOf(System.getProperty(HDFS_LOCK_ACQUIRE_TIMEOUT_KEY, String.valueOf(DEFAULT_LOCK_ACQUIRE_TIMEOUT)));
      Path monitor = getMetaLock(lockFile, lockDepth);
      boolean expired = waitUntilExpires(lockTimeout, monitor);
      if(expired) {
        removeStaleLock(lockFile, lockDepth);
      }
    }

    private void removeStaleLock(Path lockFile, int lockDepth) throws IOException {
      deleteGuard.set(Optional.of(getMetaLock(lockFile, lockDepth + 1)));
      writeLockFile(deleteGuard.get().get(), false);
      Path toDelete = lockFile;
      for(int i= 0; i<lockDepth; i++) {
        removeLock(toDelete, fs);
        toDelete = getMetaLock(toDelete);
      }
      log.info("Stale lock file deleted");
      getExecutor().schedule(this::deleteGuard, 2 * getUpdateDelay(), TimeUnit.MILLISECONDS);
    }

    private boolean waitUntilExpires(long lockTimeout, Path monitor) throws IOException {
      LockInfo before = readLockInfo(monitor, fs);
      return before.equals(expectUnchangedLockInfo(monitor, before, lockTimeout));
    }

    private LockInfo expectUnchangedLockInfo(Path monitor, LockInfo expected, long lockTimeout) throws IOException {
      LockInfo after = expected;
      long updateDelay = getUpdateDelay();
      for(int i = 0; after.equals(expected) && i <= lockTimeout/ updateDelay; i++) {
        sleeper.sleep(updateDelay);
        after = readLockInfo(monitor, fs);
      }
      return after;
    }

    private boolean deleteGuard() throws IOException {
      Optional<Path> guard = deleteGuard.getAndSet(Optional.empty());
      if(guard.isPresent())
        return fs.delete(guard.get(), false);
      return false;
    }


    private void createLockFile(boolean overwrite) throws IOException {
      writeLockFile(lockFile, overwrite);
      lastLockUpdate = System.nanoTime();
    }

    private void writeLockFile(Path lockFile, boolean overwrite) throws IOException {
      try (FSDataOutputStream file = fs.create(lockFile, overwrite)) {
        file.writeUTF(id.toString());
        file.writeLong(cycle++);
      }
    }
  }

  private long getUpdateDelay() {
    return Long.valueOf(System.getProperty(UPDATE_DELAY_KEY, String.valueOf(DEFAULT_UPDATE_DELAY)));
  }
}
