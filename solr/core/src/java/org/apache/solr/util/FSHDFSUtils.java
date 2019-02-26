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
package org.apache.solr.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Borrowed from Apache HBase to recover an HDFS lease.
 */
public class FSHDFSUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // internal, for tests
  public static AtomicLong RECOVER_LEASE_SUCCESS_COUNT = new AtomicLong();

  public interface CallerInfo {
    boolean isCallerClosed();
  }

  /**
   * Recover the lease from HDFS, retrying multiple times.
   */
  public static void recoverFileLease(final FileSystem fs, final Path p, Configuration conf, CallerInfo callerInfo) throws IOException {
    // lease recovery not needed for local file system case.
    if (!(fs instanceof DistributedFileSystem)) return;
    recoverDFSFileLease((DistributedFileSystem)fs, p, conf, callerInfo);
  }

  /*
   * Run the dfs recover lease. recoverLease is asynchronous. It returns:
   *    -false when it starts the lease recovery (i.e. lease recovery not *yet* done)
   *    - true when the lease recovery has succeeded or the file is closed.
   * But, we have to be careful.  Each time we call recoverLease, it starts the recover lease
   * process over from the beginning.  We could put ourselves in a situation where we are
   * doing nothing but starting a recovery, interrupting it to start again, and so on.
   * The findings over in HBASE-8354 have it that the namenode will try to recover the lease
   * on the file's primary node.  If all is well, it should return near immediately.  But,
   * as is common, it is the very primary node that has crashed and so the namenode will be
   * stuck waiting on a socket timeout before it will ask another datanode to start the
   * recovery. It does not help if we call recoverLease in the meantime and in particular,
   * subsequent to the socket timeout, a recoverLease invocation will cause us to start
   * over from square one (possibly waiting on socket timeout against primary node).  So,
   * in the below, we do the following:
   * 1. Call recoverLease.
   * 2. If it returns true, break.
   * 3. If it returns false, wait a few seconds and then call it again.
   * 4. If it returns true, break.
   * 5. If it returns false, wait for what we think the datanode socket timeout is
   * (configurable) and then try again.
   * 6. If it returns true, break.
   * 7. If it returns false, repeat starting at step 5. above.
   *
   * If HDFS-4525 is available, call it every second and we might be able to exit early.
   */
  static boolean recoverDFSFileLease(final DistributedFileSystem dfs, final Path p, final Configuration conf, CallerInfo callerInfo)
  throws IOException {
    log.info("Recovering lease on dfs file " + p);
    long startWaiting = System.nanoTime();
    // Default is 15 minutes. It's huge, but the idea is that if we have a major issue, HDFS
    // usually needs 10 minutes before marking the nodes as dead. So we're putting ourselves
    // beyond that limit 'to be safe'.
    long recoveryTimeout = TimeUnit.NANOSECONDS.convert(conf.getInt("solr.hdfs.lease.recovery.timeout", 900000), TimeUnit.MILLISECONDS) + startWaiting;
    // This setting should be a little bit above what the cluster dfs heartbeat is set to.
    long firstPause = conf.getInt("solr.hdfs.lease.recovery.first.pause", 4000);
    // This should be set to how long it'll take for us to timeout against primary datanode if it
    // is dead.  We set it to 61 seconds, 1 second than the default READ_TIMEOUT in HDFS, the
    // default value for DFS_CLIENT_SOCKET_TIMEOUT_KEY.
    long subsequentPause = TimeUnit.NANOSECONDS.convert(conf.getInt("solr.hdfs.lease.recovery.dfs.timeout", 61 * 1000), TimeUnit.MILLISECONDS);
    
    if (dfs.isFileClosed(p)) {
      return true;
    }
    
    boolean recovered = false;
    // We break the loop if we succeed the lease recovery, timeout, or we throw an exception.
    for (int nbAttempt = 0; !recovered; nbAttempt++) {
      recovered = recoverLease(dfs, nbAttempt, p, startWaiting);
      if (recovered) break;
      if (checkIfTimedout(conf, recoveryTimeout, nbAttempt, p, startWaiting) || callerInfo.isCallerClosed()) break;
      try {
        // On the first time through wait the short 'firstPause'.
        if (nbAttempt == 0) {
          Thread.sleep(firstPause);
        } else {
          // Cycle here until subsequentPause elapses.  While spinning, check isFileClosed
          long localStartWaiting = System.nanoTime();
          while ((System.nanoTime() - localStartWaiting) < subsequentPause && !callerInfo.isCallerClosed()) {
            Thread.sleep(conf.getInt("solr.hdfs.lease.recovery.pause", 1000));

            if (dfs.isFileClosed(p)) {
              recovered = true;
              break;
            }
          }
        }
      } catch (InterruptedException ie) {
        InterruptedIOException iioe = new InterruptedIOException();
        iioe.initCause(ie);
        throw iioe;
      }
    }
    if (recovered) {
      RECOVER_LEASE_SUCCESS_COUNT.incrementAndGet();
    }
    return recovered;
  }

  static boolean checkIfTimedout(final Configuration conf, final long recoveryTimeout,
      final int nbAttempt, final Path p, final long startWaiting) {
    if (recoveryTimeout < System.nanoTime()) {
      log.warn("Cannot recoverLease after trying for " +
        conf.getInt("solr.hdfs.lease.recovery.timeout", 900000) +
        "ms (solr.hdfs.lease.recovery.timeout); continuing, but may be DATALOSS!!!; " +
        getLogMessageDetail(nbAttempt, p, startWaiting));
      return true;
    }
    return false;
  }

  /**
   * Try to recover the lease.
   * @return True if dfs#recoverLease came by true.
   */
  static boolean recoverLease(final DistributedFileSystem dfs, final int nbAttempt, final Path p, final long startWaiting)
    throws FileNotFoundException {
    boolean recovered = false;
    try {
      recovered = dfs.recoverLease(p);
      log.info("recoverLease=" + recovered + ", " +
        getLogMessageDetail(nbAttempt, p, startWaiting));
    } catch (IOException e) {
      if (e.getMessage().contains("File does not exist")) {
        // This exception comes out instead of FNFE, fix it
        throw new FileNotFoundException("The given transactionlog file wasn't found at " + p);
      } else if (e instanceof FileNotFoundException) {
        throw (FileNotFoundException)e;
      }
      log.warn(getLogMessageDetail(nbAttempt, p, startWaiting), e);
    }
    return recovered;
  }

  /**
   * @return Detail to append to any log message around lease recovering.
   */
  private static String getLogMessageDetail(final int nbAttempt, final Path p, final long startWaiting) {
    return "attempt=" + nbAttempt + " on file=" + p + " after " +
      TimeUnit.MILLISECONDS.convert(System.nanoTime() - startWaiting, TimeUnit.NANOSECONDS) + "ms";
  }
}
