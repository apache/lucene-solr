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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexWriter that is configured via Solr config mechanisms.
 *
 * @since solr 0.9
 */

public class SolrIndexWriter extends IndexWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();

  /** Stored into each Lucene commit to record the
   *  System.currentTimeMillis() when commit was called. */
  public static final String COMMIT_TIME_MSEC_KEY = "commitTimeMSec";
  public static final String COMMIT_COMMAND_VERSION = "commitCommandVer";

  private volatile String name;
  private final DirectoryFactory directoryFactory;
  private final InfoStream infoStream;
  private volatile Directory directory;

  // metrics
  private volatile long majorMergeDocs = 512 * 1024;
  private volatile Timer majorMerge;
  private volatile Timer minorMerge;
  private volatile Meter majorMergedDocs;
  private volatile Meter majorDeletedDocs;
  private volatile Counter mergeErrors;
  private volatile Meter flushMeter; // original counter is package-private in IndexWriter
  private volatile boolean mergeTotals = false;
  private volatile boolean mergeDetails = false;
  private final AtomicInteger runningMajorMerges = new AtomicInteger();
  private final AtomicInteger runningMinorMerges = new AtomicInteger();
  private final AtomicInteger runningMajorMergesSegments = new AtomicInteger();
  private final AtomicInteger runningMinorMergesSegments = new AtomicInteger();
  private final AtomicLong runningMajorMergesDocs = new AtomicLong();
  private final AtomicLong runningMinorMergesDocs = new AtomicLong();

  private final SolrMetricsContext solrMetricsContext;
  // merge diagnostics.
  private final Map<String, Long> runningMerges = new ConcurrentHashMap<>();
  private final boolean releaseDirectory;
//
//  public static SolrIndexWriter create(SolrCore core, String name, String path, DirectoryFactory directoryFactory,
//      boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec)
//      throws IOException {
//    if (log.isDebugEnabled()) {
//      log.debug("create(SolrCore core={}, String name={}, String path={}, DirectoryFactory directoryFactory={}, boolean create={}, IndexSchema schema={}, SolrIndexConfig config={}, IndexDeletionPolicy delPolicy={}, Codec codec={}) - start",
//          core, name, path, directoryFactory, create, schema, config, delPolicy, codec);
//    }
//
//    SolrIndexWriter w = null;
//
//    w = new SolrIndexWriter(core, name, path, directoryFactory, create, schema, config, delPolicy, codec);
//
//    if (log.isDebugEnabled()) {
//      log.debug(
//          "create(SolrCore, String, String, DirectoryFactory, boolean, IndexSchema, SolrIndexConfig, IndexDeletionPolicy, Codec) - end");
//    }
//    return w;
//  }

  public static SolrIndexWriter buildIndexWriter(SolrCore core, String name, String path, DirectoryFactory directoryFactory, boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec) {
    SolrIndexWriter iw = null;
    Directory dir = null;
    try {
      dir = getDir(directoryFactory, path, config);
      iw = new SolrIndexWriter(core, name, directoryFactory, dir, create, schema, config, delPolicy, codec);
    } catch (Throwable e) {
      ParWork.propegateInterrupt(e);
      SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);

      if (iw != null) {
        try {
          iw.close();
        } catch (Exception e1) {
          if (dir != null) {
            try {
              directoryFactory.release(dir);
            } catch (IOException e2) {
              exp.addSuppressed(e2);
            }
          }
          exp.addSuppressed(e1);
        }
      } else {
        if (dir != null) {
          try {
            directoryFactory.release(dir);
          } catch (IOException e1) {
            exp.addSuppressed(e1);
          }
        }
      }
      if (e instanceof  Error) {
        log.error("Exception constructing SolrIndexWriter", exp);
        throw (Error) e;
      }
      throw exp;
    }

    return iw;
  }

  public SolrIndexWriter(String name, Directory d, IndexWriterConfig conf) throws IOException {
    super(d, conf);
    this.name = name;
    this.infoStream = conf.getInfoStream();
    this.directory = d;
    this.directoryFactory = null;
    numOpens.incrementAndGet();
    if (log.isDebugEnabled()) log.debug("Opened Writer " + name);
    // no metrics
    mergeTotals = false;
    mergeDetails = false;
    solrMetricsContext = null;
    this.releaseDirectory=false;
    assert ObjectReleaseTracker.track(this);
  }

  public SolrIndexWriter(SolrCore core, String name, DirectoryFactory directoryFactory, Directory directory, boolean create, IndexSchema schema, SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec) throws IOException {
    super(directory,
            config.toIndexWriterConfig(core).
                    setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND).
                    setIndexDeletionPolicy(delPolicy).setCodec(codec)
    );
    try {
    if (log.isDebugEnabled()) log.debug("Opened Writer " + name);
      this.releaseDirectory = true;
      this.directory = getDirectory();
      this.directoryFactory = directoryFactory;
      this.name = name;
      infoStream = getConfig().getInfoStream();
      numOpens.incrementAndGet();
      solrMetricsContext = core.getSolrMetricsContext().getChildContext(this);
      if (config.metricsInfo != null && config.metricsInfo.initArgs != null) {
        Object v = config.metricsInfo.initArgs.get("majorMergeDocs");
        if (v != null) {
          try {
            majorMergeDocs = Long.parseLong(String.valueOf(v));
          } catch (Exception e) {
            log.warn("Invalid 'majorMergeDocs' argument, using default 512k", e);
          }
        }
        Boolean Totals = config.metricsInfo.initArgs.getBooleanArg("merge");
        Boolean Details = config.metricsInfo.initArgs.getBooleanArg("mergeDetails");
        if (Details != null) {
          mergeDetails = Details;
        } else {
          mergeDetails = false;
        }
        if (Totals != null) {
          mergeTotals = Totals;
        } else {
          mergeTotals = false;
        }
        if (mergeDetails) {
          mergeTotals = true; // override
          majorMergedDocs = solrMetricsContext.meter("docs", SolrInfoBean.Category.INDEX.toString(), "merge", "major");
          majorDeletedDocs = solrMetricsContext.meter("deletedDocs", SolrInfoBean.Category.INDEX.toString(), "merge", "major");
        }
        if (mergeTotals) {
          minorMerge = solrMetricsContext.timer("minor", SolrInfoBean.Category.INDEX.toString(), "merge");
          majorMerge = solrMetricsContext.timer("major", SolrInfoBean.Category.INDEX.toString(), "merge");
          mergeErrors = solrMetricsContext.counter("errors", SolrInfoBean.Category.INDEX.toString(), "merge");
          String tag = core.getMetricTag();
          solrMetricsContext.gauge(() -> runningMajorMerges.get(), true, "running", SolrInfoBean.Category.INDEX.toString(), "merge", "major");
          solrMetricsContext.gauge(() -> runningMinorMerges.get(), true, "running", SolrInfoBean.Category.INDEX.toString(), "merge", "minor");
          solrMetricsContext.gauge(() -> runningMajorMergesDocs.get(), true, "running.docs", SolrInfoBean.Category.INDEX.toString(), "merge", "major");
          solrMetricsContext.gauge(() -> runningMinorMergesDocs.get(), true, "running.docs", SolrInfoBean.Category.INDEX.toString(), "merge", "minor");
          solrMetricsContext.gauge(() -> runningMajorMergesSegments.get(), true, "running.segments", SolrInfoBean.Category.INDEX.toString(), "merge", "major");
          solrMetricsContext.gauge(() -> runningMinorMergesSegments.get(), true, "running.segments", SolrInfoBean.Category.INDEX.toString(), "merge", "minor");
          flushMeter = solrMetricsContext.meter("flush", SolrInfoBean.Category.INDEX.toString());
        }
      }
    } catch (Exception e) {
      directoryFactory.release(getDirectory());
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating IndexWriter");
    }
    assert ObjectReleaseTracker.track(this);
  }

  public static Directory getDir(DirectoryFactory directoryFactory, String path, SolrIndexConfig config) {
    Directory dir = null;
    try {
      dir = directoryFactory.get(path,  DirContext.DEFAULT, config.lockType);
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      if (dir != null) try {
        directoryFactory.release(dir);
      } catch (IOException e1) {
        exp.addSuppressed(e1);
      }
      throw exp;
    }
    return dir;
  }

  @SuppressForbidden(reason = "Need currentTimeMillis, commit time should be used only for debugging purposes, " +
          " but currently suspiciously used for replication as well")
  public static void setCommitData(IndexWriter iw, long commitCommandVersion) {
    log.info("Calling setCommitData with IW:" + iw.toString() + " commitCommandVersion:"+commitCommandVersion);
    final Map<String,String> commitData = new HashMap<>();
    commitData.put(COMMIT_TIME_MSEC_KEY, String.valueOf(System.currentTimeMillis()));
    commitData.put(COMMIT_COMMAND_VERSION, String.valueOf(commitCommandVersion));
    iw.setLiveCommitData(commitData.entrySet());

    if (log.isDebugEnabled()) {
      log.debug("setCommitData(IndexWriter, long) - end");
    }
  }

  // we override this method to collect metrics for merges.
  @Override
  public void merge(MergePolicy.OneMerge merge) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("merge(MergePolicy.OneMerge merge={}) - start", merge);
    }

    String segString = merge.segString();
    long totalNumDocs = merge.totalNumDocs();
    runningMerges.put(segString, totalNumDocs);
    if (!mergeTotals) {
      try {
        super.merge(merge);
      } finally {
        runningMerges.remove(segString);
      }

      if (log.isDebugEnabled()) {
        log.debug("merge(MergePolicy.OneMerge) - end");
      }
      return;
    }
    long deletedDocs = 0;
    for (SegmentCommitInfo info : merge.segments) {
      totalNumDocs -= info.getDelCount();
      deletedDocs += info.getDelCount();
    }
    boolean major = totalNumDocs > majorMergeDocs;
    int segmentsCount = merge.segments.size();
    Timer.Context context;
    if (major) {
      runningMajorMerges.incrementAndGet();
      runningMajorMergesDocs.addAndGet(totalNumDocs);
      runningMajorMergesSegments.addAndGet(segmentsCount);
      if (mergeDetails) {
        majorMergedDocs.mark(totalNumDocs);
        majorDeletedDocs.mark(deletedDocs);
      }
      context = majorMerge.time();
    } else {
      runningMinorMerges.incrementAndGet();
      runningMinorMergesDocs.addAndGet(totalNumDocs);
      runningMinorMergesSegments.addAndGet(segmentsCount);
      context = minorMerge.time();
    }
    try {
      super.merge(merge);
    } catch (Throwable t) {
      mergeErrors.inc();
      throw t;
    } finally {
      runningMerges.remove(segString);
      context.stop();
      if (major) {
        runningMajorMerges.decrementAndGet();
        runningMajorMergesDocs.addAndGet(-totalNumDocs);
        runningMajorMergesSegments.addAndGet(-segmentsCount);
      } else {
        runningMinorMerges.decrementAndGet();
        runningMinorMergesDocs.addAndGet(-totalNumDocs);
        runningMinorMergesSegments.addAndGet(-segmentsCount);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("merge(MergePolicy.OneMerge) - end");
    }
  }

  public Map<String, Object> getRunningMerges() {
    if (log.isDebugEnabled()) {
      log.debug("getRunningMerges() - start");
    }

    Map<String,Object> returnMap = Collections.unmodifiableMap(runningMerges);
    if (log.isDebugEnabled()) {
      log.debug("getRunningMerges() - end");
    }
    return returnMap;
  }

  @Override
  protected void doAfterFlush() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("doAfterFlush() - start");
    }

    if (flushMeter != null) { // this is null when writer is used only for snapshot cleanup
      flushMeter.mark();      // or if mergeTotals == false
    }
    super.doAfterFlush();

    if (log.isDebugEnabled()) {
      log.debug("doAfterFlush() - end");
    }
  }

  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled()) log.debug("Closing Writer " + name);
    try {
      super.close();
    } catch (Throwable e) {
      ParWork.propegateInterrupt("Error closing IndexWriter", e);
    } finally {
      cleanup("close");
    }

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  @Override
  public void rollback() throws IOException {
    if (log.isDebugEnabled())  log.debug("Rollback Writer " + name);
    try {
      super.rollback();
    } catch (Throwable e) {
      ParWork.propegateInterrupt("Exception rolling back IndexWriter", e);
    } finally {
      cleanup("rollback");
    }

    if (log.isDebugEnabled()) {
      log.debug("rollback() - end");
    }
  }

  private void cleanup(String label) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("cleanup() - start");
    }
    numCloses.incrementAndGet();

    log.info("SolrIndexWriter close {} numCloses={}", label, numCloses.get());
    try {
      if (infoStream != null) {
        ParWork.close(infoStream, true);
      }

      if (releaseDirectory) {
        log.info("SolrIndexWriter release {}", directory);
        directoryFactory.release(directory);
      }
      if (solrMetricsContext != null) {
        solrMetricsContext.unregister();
      }
    } finally {
      assert ObjectReleaseTracker.release(this);
    }

    if (log.isDebugEnabled()) {
      log.debug("cleanup() - end");
    }
  }
}
