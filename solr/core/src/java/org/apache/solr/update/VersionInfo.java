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

import org.apache.lucene.util.BitUtil;
import org.apache.solr.schema.SchemaField;

import java.util.concurrent.atomic.AtomicLong;

public class VersionInfo {
  public static final String VERSION_FIELD="_version_";

  private final VersionBucket[] buckets;
  private SchemaField versionField;

  public VersionInfo(UpdateHandler updateHandler, int nBuckets) {
    versionField = updateHandler.core.getSchema().getFieldOrNull("_version_");
    buckets = new VersionBucket[ BitUtil.nextHighestPowerOfTwo(nBuckets) ];
    for (int i=0; i<buckets.length; i++) {
      buckets[i] = new VersionBucket();
    }
  }

  public SchemaField getVersionField() {
    return versionField;
  }


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

  /***
  // Time-based lamport clock.  Good for introducing some reality into clocks (to the degree
  // that times are somewhat synchronized in the cluster).
  // Good if we want to relax some constraints to scale down to where only one node may be
  // up at a time.  Harder to detect missing messages (because versions are not contiguous.
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
  ***/

  public VersionBucket bucket(int hash) {
    // If this is a user provided hash, it may be poor in the right-hand bits.
    // Make sure high bits are moved down, since only the low bits will matter.
    // int h = hash + (hash >>> 8) + (hash >>> 16) + (hash >>> 24);
    // Assume good hash codes for now.

    int slot = hash & (buckets.length-1);
    return buckets[slot];
  }

}
