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
package org.apache.solr.update;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

public class VersionInfo {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SYS_PROP_BUCKET_VERSION_LOCK_TIMEOUT_MS = "bucketVersionLockTimeoutMs";

  private final UpdateLog ulog;
  private final VersionBucket[] buckets;
  private SchemaField versionField;
  final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private int versionBucketLockTimeoutMs;

  /**
   * Gets and returns the {@link org.apache.solr.common.params.CommonParams#VERSION_FIELD} from the specified
   * schema, after verifying that it is indexed, stored, and single-valued.  
   * If any of these pre-conditions are not met, it throws a SolrException 
   * with a user suitable message indicating the problem.
   */
  public static SchemaField getAndCheckVersionField(IndexSchema schema) 
    throws SolrException {
    final String errPrefix = VERSION_FIELD + " field must exist in schema and be searchable (indexed or docValues) and retrievable(stored or docValues) and not multiValued";
    SchemaField sf = schema.getFieldOrNull(VERSION_FIELD);

    if (null == sf) {
      throw new SolrException
        (SolrException.ErrorCode.SERVER_ERROR, 
         errPrefix + " (" + VERSION_FIELD + " does not exist)");
    }
    if ( !sf.indexed() && !sf.hasDocValues()) {
      throw new SolrException
        (SolrException.ErrorCode.SERVER_ERROR, 
         errPrefix + " (" + VERSION_FIELD + " not searchable");
    }
    if ( !sf.stored() && !sf.hasDocValues()) {
      throw new SolrException
        (SolrException.ErrorCode.SERVER_ERROR, 
         errPrefix + " (" + VERSION_FIELD + " not retrievable");
    }
    if ( sf.multiValued() ) {
      throw new SolrException
        (SolrException.ErrorCode.SERVER_ERROR, 
         errPrefix + " (" + VERSION_FIELD + " is multiValued");
    }
    
    return sf;
  }

  public VersionInfo(UpdateLog ulog, int nBuckets) {
    this.ulog = ulog;
    IndexSchema schema = ulog.uhandler.core.getLatestSchema(); 
    versionField = getAndCheckVersionField(schema);
    versionBucketLockTimeoutMs = ulog.uhandler.core.getSolrConfig().get("updateHandler").get("versionBucketLockTimeoutMs")
        .intVal(Integer.parseInt(System.getProperty(SYS_PROP_BUCKET_VERSION_LOCK_TIMEOUT_MS, "0")));
    buckets = new VersionBucket[ BitUtil.nextHighestPowerOfTwo(nBuckets) ];
    for (int i=0; i<buckets.length; i++) {
      if (versionBucketLockTimeoutMs > 0) {
        buckets[i] = new TimedVersionBucket();
      } else {
        buckets[i] = new VersionBucket();
      }
    }
  }
  
  public int getVersionBucketLockTimeoutMs() {
    return versionBucketLockTimeoutMs;
  }

  public void reload() {
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
  // up at a time.  Possibly harder to detect missing messages (because versions are not contiguous).
  private long vclock;
  private final Object clockSync = new Object();

  @SuppressForbidden(reason = "need currentTimeMillis just for getting realistic version stamps, does not assume monotonicity")
  public long getNewClock() {
    synchronized (clockSync) {
      long time = System.currentTimeMillis();
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
    return ulog.lookupVersion(idBytes);
  }

  /**
   * Returns the latest version from the index, searched by the given id (bytes) as seen from the realtime searcher.
   * Returns null if no document can be found in the index for the given id.
   */
  @SuppressWarnings({"unchecked"})
  public Long getVersionFromIndex(BytesRef idBytes) {
    // TODO: we could cache much of this and invalidate during a commit.
    // TODO: most DocValues classes are threadsafe - expose which.

    RefCounted<SolrIndexSearcher> newestSearcher = ulog.uhandler.core.getRealtimeSearcher();
    try {
      SolrIndexSearcher searcher = newestSearcher.get();
      long lookup = searcher.lookupId(idBytes);
      if (lookup < 0) return null; // this means the doc doesn't exist in the index yet

      ValueSource vs = versionField.getType().getValueSource(versionField, null);
      @SuppressWarnings({"rawtypes"})
      Map context = ValueSource.newContext(searcher);
      vs.createWeight(context, searcher);
      FunctionValues fv = vs.getValues(context, searcher.getTopReaderContext().leaves().get((int) (lookup >> 32)));
      long ver = fv.longVal((int) lookup);
      return ver;

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error reading version from index", e);
    } finally {
      if (newestSearcher != null) {
        newestSearcher.decref();
      }
    }
  }

  /**
   * Returns the highest version from the index, or 0L if no versions can be found in the index.
   */
  @SuppressWarnings({"unchecked"})
  public Long getMaxVersionFromIndex(IndexSearcher searcher) throws IOException {

    final String versionFieldName = versionField.getName();

    log.debug("Refreshing highest value of {} for {} version buckets from index", versionFieldName, buckets.length);
    // if indexed, then we have terms to get the max from
    if (versionField.indexed()) {
      if (versionField.getType().isPointField()) {
        return getMaxVersionFromIndexedPoints(searcher);
      } else {
        return getMaxVersionFromIndexedTerms(searcher);
      }
    }
    // else: not indexed, use docvalues via value source ...
    
    long maxVersionInIndex = 0L;
    ValueSource vs = versionField.getType().getValueSource(versionField, null);
    @SuppressWarnings({"rawtypes"})
    Map funcContext = ValueSource.newContext(searcher);
    vs.createWeight(funcContext, searcher);
    // TODO: multi-thread this
    for (LeafReaderContext ctx : searcher.getTopReaderContext().leaves()) {
      int maxDoc = ctx.reader().maxDoc();
      FunctionValues fv = vs.getValues(funcContext, ctx);
      for (int doc = 0; doc < maxDoc; doc++) {
        long v = fv.longVal(doc);
        maxVersionInIndex = Math.max(v, maxVersionInIndex);
      }
    }
    return maxVersionInIndex;
  }

  public void seedBucketsWithHighestVersion(long highestVersion) {
    for (int i=0; i<buckets.length; i++) {
      // should not happen, but in case other threads are calling updateHighest on the version bucket
      synchronized (buckets[i]) {
        if (buckets[i].highest < highestVersion)
          buckets[i].highest = highestVersion;
      }
    }
  }

  private long getMaxVersionFromIndexedTerms(IndexSearcher searcher) throws IOException {
    assert ! versionField.getType().isPointField();
      
    final String versionFieldName = versionField.getName();
    final LeafReader leafReader = SlowCompositeReaderWrapper.wrap(searcher.getIndexReader());
    final Terms versionTerms = leafReader.terms(versionFieldName);
    final Long max = (versionTerms != null) ? LegacyNumericUtils.getMaxLong(versionTerms) : null;
    if (null != max) {
      log.debug("Found MAX value {} from Terms for {} in index", max, versionFieldName);
      return max.longValue();
    }
    return 0L;
  }
  
  private long getMaxVersionFromIndexedPoints(IndexSearcher searcher) throws IOException {
    assert versionField.getType().isPointField();
    
    final String versionFieldName = versionField.getName();
    final byte[] maxBytes = PointValues.getMaxPackedValue(searcher.getIndexReader(), versionFieldName);
    if (null == maxBytes) {
      return 0L;
    }
    final Object maxObj = versionField.getType().toObject(versionField, new BytesRef(maxBytes));
    if (null == maxObj || ! ( maxObj instanceof Number) ) {
      // HACK: aparently nothing asserts that the FieldType is numeric (let alone a Long???)
      log.error("Unable to convert MAX byte[] from Points for {} in index", versionFieldName);
      return 0L;
    }
    
    final long max = ((Number)maxObj).longValue();
    log.debug("Found MAX value {} from Points for {} in index", max, versionFieldName);
    return max;
  }
}
