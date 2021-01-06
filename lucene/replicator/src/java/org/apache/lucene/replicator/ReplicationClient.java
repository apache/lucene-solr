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
package org.apache.lucene.replicator;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A client which monitors and obtains new revisions from a {@link Replicator}. It can be used to
 * either periodically check for updates by invoking {@link #startUpdateThread}, or manually by
 * calling {@link #updateNow()}.
 *
 * <p>Whenever a new revision is available, the {@link #requiredFiles(Map)} are copied to the {@link
 * Directory} specified by {@link PerSessionDirectoryFactory} and a handler is notified.
 *
 * @lucene.experimental
 */
public class ReplicationClient implements Closeable {

  private class ReplicationThread extends Thread {

    private final long interval;

    // client uses this to stop us
    final CountDownLatch stop = new CountDownLatch(1);

    public ReplicationThread(long interval) {
      this.interval = interval;
    }

    @SuppressWarnings("synthetic-access")
    @Override
    public void run() {
      while (true) {
        long time = System.currentTimeMillis();
        updateLock.lock();
        try {
          doUpdate();
        } catch (Throwable t) {
          handleUpdateException(t);
        } finally {
          updateLock.unlock();
        }
        time = System.currentTimeMillis() - time;

        // adjust timeout to compensate the time spent doing the replication.
        final long timeout = interval - time;
        if (timeout > 0) {
          try {
            // this will return immediately if we were ordered to stop (count=0)
            // or the timeout has elapsed. if it returns true, it means count=0,
            // so terminate.
            if (stop.await(timeout, TimeUnit.MILLISECONDS)) {
              return;
            }
          } catch (InterruptedException e) {
            // if we were interruted, somebody wants to terminate us, so just
            // throw the exception further.
            Thread.currentThread().interrupt();
            throw new ThreadInterruptedException(e);
          }
        }
      }
    }
  }

  /** Handler for revisions obtained by the client. */
  public static interface ReplicationHandler {

    /** Returns the current revision files held by the handler. */
    public Map<String, List<RevisionFile>> currentRevisionFiles();

    /** Returns the current revision version held by the handler. */
    public String currentVersion();

    /**
     * Called when a new revision was obtained and is available (i.e. all needed files were
     * successfully copied).
     *
     * @param version the version of the {@link Revision} that was copied
     * @param revisionFiles the files contained by this {@link Revision}
     * @param copiedFiles the files that were actually copied
     * @param sourceDirectory a mapping from a source of files to the {@link Directory} they were
     *     copied into
     */
    public void revisionReady(
        String version,
        Map<String, List<RevisionFile>> revisionFiles,
        Map<String, List<String>> copiedFiles,
        Map<String, Directory> sourceDirectory)
        throws IOException;
  }

  /**
   * Resolves a session and source into a {@link Directory} to use for copying the session files to.
   */
  public static interface SourceDirectoryFactory {

    /**
     * Called to denote that the replication actions for this session were finished and the
     * directory is no longer needed.
     */
    public void cleanupSession(String sessionID) throws IOException;

    /**
     * Returns the {@link Directory} to use for the given session and source. Implementations may
     * e.g. return different directories for different sessions, or the same directory for all
     * sessions. In that case, it is advised to clean the directory before it is used for a new
     * session.
     *
     * @see #cleanupSession(String)
     */
    public Directory getDirectory(String sessionID, String source) throws IOException;
  }

  /** The component name to use with {@link InfoStream#isEnabled(String)}. */
  public static final String INFO_STREAM_COMPONENT = "ReplicationThread";

  private final Replicator replicator;
  private final ReplicationHandler handler;
  private final SourceDirectoryFactory factory;
  private final byte[] copyBuffer = new byte[16384];
  private final Lock updateLock = new ReentrantLock();

  private volatile ReplicationThread updateThread;
  private volatile boolean closed = false;
  private volatile InfoStream infoStream = InfoStream.getDefault();

  /**
   * Constructor.
   *
   * @param replicator the {@link Replicator} used for checking for updates
   * @param handler notified when new revisions are ready
   * @param factory returns a {@link Directory} for a given source and session
   */
  public ReplicationClient(
      Replicator replicator, ReplicationHandler handler, SourceDirectoryFactory factory) {
    this.replicator = replicator;
    this.handler = handler;
    this.factory = factory;
  }

  private void copyBytes(IndexOutput out, InputStream in) throws IOException {
    int numBytes;
    while ((numBytes = in.read(copyBuffer)) > 0) {
      out.writeBytes(copyBuffer, 0, numBytes);
    }
  }

  private void doUpdate() throws IOException {
    SessionToken session = null;
    final Map<String, Directory> sourceDirectory = new HashMap<>();
    final Map<String, List<String>> copiedFiles = new HashMap<>();
    boolean notify = false;
    try {
      final String version = handler.currentVersion();
      session = replicator.checkForUpdate(version);
      if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
        infoStream.message(
            INFO_STREAM_COMPONENT, "doUpdate(): handlerVersion=" + version + " session=" + session);
      }
      if (session == null) {
        // already up to date
        return;
      }
      Map<String, List<RevisionFile>> requiredFiles = requiredFiles(session.sourceFiles);
      if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
        infoStream.message(INFO_STREAM_COMPONENT, "doUpdate(): requiredFiles=" + requiredFiles);
      }
      for (Entry<String, List<RevisionFile>> e : requiredFiles.entrySet()) {
        String source = e.getKey();
        Directory dir = factory.getDirectory(session.id, source);
        sourceDirectory.put(source, dir);
        List<String> cpFiles = new ArrayList<>();
        copiedFiles.put(source, cpFiles);
        for (RevisionFile file : e.getValue()) {
          if (closed) {
            // if we're closed, abort file copy
            if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
              infoStream.message(
                  INFO_STREAM_COMPONENT,
                  "doUpdate(): detected client was closed); abort file copy");
            }
            return;
          }
          InputStream in = null;
          IndexOutput out = null;
          try {
            in = replicator.obtainFile(session.id, source, file.fileName);
            out = dir.createOutput(file.fileName, IOContext.DEFAULT);
            copyBytes(out, in);
            cpFiles.add(file.fileName);
            // TODO add some validation, on size / checksum
          } finally {
            IOUtils.close(in, out);
          }
        }
      }
      // only notify if all required files were successfully obtained.
      notify = true;
    } finally {
      if (session != null) {
        try {
          replicator.release(session.id);
        } finally {
          if (!notify) { // cleanup after ourselves
            IOUtils.close(sourceDirectory.values());
            factory.cleanupSession(session.id);
          }
        }
      }
    }

    // notify outside the try-finally above, so the session is released sooner.
    // the handler may take time to finish acting on the copied files, but the
    // session itself is no longer needed.
    try {
      if (notify && !closed) { // no use to notify if we are closed already
        handler.revisionReady(session.version, session.sourceFiles, copiedFiles, sourceDirectory);
      }
    } finally {
      IOUtils.close(sourceDirectory.values());
      if (session != null) {
        factory.cleanupSession(session.id);
      }
    }
  }

  /** Throws {@link AlreadyClosedException} if the client has already been closed. */
  protected final void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("this update client has already been closed");
    }
  }

  /**
   * Called when an exception is hit by the replication thread. The default implementation prints
   * the full stacktrace to the {@link InfoStream} set in {@link #setInfoStream(InfoStream)}, or the
   * {@link InfoStream#getDefault() default} one. You can override to log the exception elswhere.
   *
   * <p><b>NOTE:</b> if you override this method to throw the exception further, the replication
   * thread will be terminated. The only way to restart it is to call {@link #stopUpdateThread()}
   * followed by {@link #startUpdateThread(long, String)}.
   */
  protected void handleUpdateException(Throwable t) {
    final StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
      infoStream.message(
          INFO_STREAM_COMPONENT, "an error occurred during revision update: " + sw.toString());
    }
  }

  /**
   * Returns the files required for replication. By default, this method returns all files that
   * exist in the new revision, but not in the handler.
   */
  protected Map<String, List<RevisionFile>> requiredFiles(
      Map<String, List<RevisionFile>> newRevisionFiles) {
    Map<String, List<RevisionFile>> handlerRevisionFiles = handler.currentRevisionFiles();
    if (handlerRevisionFiles == null) {
      return newRevisionFiles;
    }

    Map<String, List<RevisionFile>> requiredFiles = new HashMap<>();
    for (Entry<String, List<RevisionFile>> e : handlerRevisionFiles.entrySet()) {
      // put the handler files in a Set, for faster contains() checks later
      Set<String> handlerFiles = new HashSet<>();
      for (RevisionFile file : e.getValue()) {
        handlerFiles.add(file.fileName);
      }

      // make sure to preserve revisionFiles order
      ArrayList<RevisionFile> res = new ArrayList<>();
      String source = e.getKey();
      assert newRevisionFiles.containsKey(source)
          : "source not found in newRevisionFiles: " + newRevisionFiles;
      for (RevisionFile file : newRevisionFiles.get(source)) {
        if (!handlerFiles.contains(file.fileName)) {
          res.add(file);
        }
      }
      requiredFiles.put(source, res);
    }

    return requiredFiles;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      stopUpdateThread();
      closed = true;
    }
  }

  /**
   * Start the update thread with the specified interval in milliseconds. For debugging purposes,
   * you can optionally set the name to set on {@link Thread#setName(String)}. If you pass {@code
   * null}, a default name will be set.
   *
   * @throws IllegalStateException if the thread has already been started
   */
  public synchronized void startUpdateThread(long intervalMillis, String threadName) {
    ensureOpen();
    if (updateThread != null && updateThread.isAlive()) {
      throw new IllegalStateException(
          "cannot start an update thread when one is running, must first call 'stopUpdateThread()'");
    }
    threadName = threadName == null ? INFO_STREAM_COMPONENT : "ReplicationThread-" + threadName;
    updateThread = new ReplicationThread(intervalMillis);
    updateThread.setName(threadName);
    updateThread.start();
    // we rely on isAlive to return true in isUpdateThreadAlive, assert to be on the safe side
    assert updateThread.isAlive() : "updateThread started but not alive?";
  }

  /**
   * Stop the update thread. If the update thread is not running, silently does nothing. This method
   * returns after the update thread has stopped.
   */
  public synchronized void stopUpdateThread() {
    if (updateThread != null) {
      // this will trigger the thread to terminate if it awaits the lock.
      // otherwise, if it's in the middle of replication, we wait for it to
      // stop.
      updateThread.stop.countDown();
      try {
        updateThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ThreadInterruptedException(e);
      }
      updateThread = null;
    }
  }

  /**
   * Returns true if the update thread is alive. The update thread is alive if it has been {@link
   * #startUpdateThread(long, String) started} and not {@link #stopUpdateThread() stopped}, as well
   * as didn't hit an error which caused it to terminate (i.e. {@link
   * #handleUpdateException(Throwable)} threw the exception further).
   */
  public synchronized boolean isUpdateThreadAlive() {
    return updateThread != null && updateThread.isAlive();
  }

  @Override
  public String toString() {
    String res = "ReplicationClient";
    if (updateThread != null) {
      res += " (" + updateThread.getName() + ")";
    }
    return res;
  }

  /**
   * Executes the update operation immediately, irregardless if an update thread is running or not.
   */
  public void updateNow() throws IOException {
    ensureOpen();
    updateLock.lock();
    try {
      doUpdate();
    } finally {
      updateLock.unlock();
    }
  }

  /** Sets the {@link InfoStream} to use for logging messages. */
  public void setInfoStream(InfoStream infoStream) {
    if (infoStream == null) {
      infoStream = InfoStream.NO_OUTPUT;
    }
    this.infoStream = infoStream;
  }
}
