/**
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

package org.apache.solr.update;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

public class VersionInfo {
  public static final String VERSION_FIELD="_version_";

  private SolrCore core;
  private UpdateHandler updateHandler;
  private final VersionBucket[] buckets;
  private SchemaField versionField;
  private SchemaField idField;
  final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  public VersionInfo(UpdateHandler updateHandler, int nBuckets) {
    this.updateHandler = updateHandler;
    this.core = updateHandler.core;
    versionField = core.getSchema().getFieldOrNull(VERSION_FIELD);
    idField = core.getSchema().getUniqueKeyField();
    buckets = new VersionBucket[ BitUtil.nextHighestPowerOfTwo(nBuckets) ];
    for (int i=0; i<buckets.length; i++) {
      buckets[i] = new VersionBucket();
    }
  }

  public SchemaField getVersionField() {
    return versionField;
  }

  public void lockForUpdate() {
    lock.readLock().lock();
  }

  public void unlockForUpdate() {
    lock.readLock().unlock();
  }

  public void blockUpdates() {
    lock.writeLock().lock();
  }

  public void unblockUpdates() {
    lock.writeLock().unlock();
  }

  /***
  // todo: initialize... use current time to start?
  // a clock that increments by 1 for every operation makes it easier to detect missing
  // messages, but raises other issues:
  // - need to initialize to largest thing in index or tlog
  // - when becoming leader, need to make sure it's greater than
  // - using to detect missing messages means we need to keep track per-leader, or make
  //   sure a new leader starts off with 1 greater than the last leader.
  private final AtomicLong clock = new AtomicLong();

  public long getNewClock() {
    return clock.incrementAndGet();
  }

  // Named *old* to prevent accidental calling getClock and expecting a new updated clock.
  public long getOldClock() {
    return clock.get();
  }
  ***/

  /** We are currently using this time-based clock to avoid going back in time on a
   * server restart (i.e. we don't want version numbers to start at 1 again).
   */

  // Time-based lamport clock.  Good for introducing some reality into clocks (to the degree
  // that times are somewhat synchronized in the cluster).
  // Good if we want to relax some constraints to scale down to where only one node may be
  // up at a time.  Possibly harder to detect missing messages (because versions are not contiguous.
  long vclock;
  long time;
  private final Object clockSync = new Object();


  public long getNewClock() {
    synchronized (clockSync) {
      time = System.currentTimeMillis();
      long result = time << 20;
      if (result <= vclock) {
        result = vclock + 1;
      }
      vclock = result;
      return vclock;
    }
  }

  public long getOldClock() {
    synchronized (clockSync) {
      return vclock;
    }
  }

  public void updateClock(long clock) {
    synchronized (clockSync) {
      vclock = Math.max(vclock, clock);
    }
  }


  public VersionBucket bucket(int hash) {
    // If this is a user provided hash, it may be poor in the right-hand bits.
    // Make sure high bits are moved down, since only the low bits will matter.
    // int h = hash + (hash >>> 8) + (hash >>> 16) + (hash >>> 24);
    // Assume good hash codes for now.

    int slot = hash & (buckets.length-1);
    return buckets[slot];
  }

  public Long lookupVersion(BytesRef idBytes) {
    return updateHandler.ulog.lookupVersion(idBytes);
  }

  public Long getVersionFromIndex(BytesRef idBytes) {
    // TODO: we could cache much of this and invalidate during a commit.
    // TODO: most DocValues classes are threadsafe - expose which.

    RefCounted<SolrIndexSearcher> newestSearcher = core.getRealtimeSearcher();
    try {
      SolrIndexSearcher searcher = newestSearcher.get();
      long lookup = searcher.lookupId(idBytes);
      if (lookup < 0) return null;

      ValueSource vs = versionField.getType().getValueSource(versionField, null);
      Map context = ValueSource.newContext(searcher);
      vs.createWeight(context, searcher);
      FunctionValues fv = vs.getValues(context, searcher.getTopReaderContext().leaves()[(int)(lookup>>32)]);
      long ver = fv.longVal((int)lookup);
      return ver;

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading version from index", e);
    } finally {
      if (newestSearcher != null) {
        newestSearcher.decref();
      }
    }
  }

}
