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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.Meter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.SolrConfig.UpdateHandlerInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionRangeQuery;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>DirectUpdateHandler2</code> implements an UpdateHandler where documents are added
 * directly to the main Lucene index as opposed to adding to a separate smaller index.
 */
public class DirectUpdateHandler2 extends UpdateHandler implements SolrCoreState.IndexWriterCloser, SolrMetricProducer {

  private static final int NO_FILE_SIZE_UPPER_BOUND_PLACEHOLDER = -1;

  protected final SolrCoreState solrCoreState;

  // stats
  LongAdder addCommands = new LongAdder();
  Meter addCommandsCumulative;
  LongAdder deleteByIdCommands= new LongAdder();
  Meter deleteByIdCommandsCumulative;
  LongAdder deleteByQueryCommands = new LongAdder();
  Meter deleteByQueryCommandsCumulative;
  Meter expungeDeleteCommands;
  Meter mergeIndexesCommands;
  Meter commitCommands;
  Meter splitCommands;
  Meter optimizeCommands;
  Meter rollbackCommands;
  LongAdder numDocsPending = new LongAdder();
  LongAdder numErrors = new LongAdder();
  Meter numErrorsCumulative;

  // tracks when auto-commit should occur
  protected final CommitTracker commitTracker;
  protected final CommitTracker softCommitTracker;
  
  protected boolean commitWithinSoftCommit;
  /**
   * package access for testing
   * @lucene.internal
   */
  void setCommitWithinSoftCommit(boolean value) {
    this.commitWithinSoftCommit = value;
  }

  protected boolean indexWriterCloseWaitsForMerges;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public DirectUpdateHandler2(SolrCore core) {
    super(core);
   
    solrCoreState = core.getSolrCoreState();
    
    UpdateHandlerInfo updateHandlerInfo = core.getSolrConfig()
        .getUpdateHandlerInfo();
    int docsUpperBound = updateHandlerInfo.autoCommmitMaxDocs;
    int timeUpperBound = updateHandlerInfo.autoCommmitMaxTime;
    long fileSizeUpperBound = updateHandlerInfo.autoCommitMaxSizeBytes;
    commitTracker = new CommitTracker("Hard", core, docsUpperBound, timeUpperBound, fileSizeUpperBound, updateHandlerInfo.openSearcher, false);
    
    int softCommitDocsUpperBound = updateHandlerInfo.autoSoftCommmitMaxDocs;
    int softCommitTimeUpperBound = updateHandlerInfo.autoSoftCommmitMaxTime;
    softCommitTracker = new CommitTracker("Soft", core, softCommitDocsUpperBound, softCommitTimeUpperBound, NO_FILE_SIZE_UPPER_BOUND_PLACEHOLDER, true, true);
    
    commitWithinSoftCommit = updateHandlerInfo.commitWithinSoftCommit;
    indexWriterCloseWaitsForMerges = updateHandlerInfo.indexWriterCloseWaitsForMerges;

    ZkController zkController = core.getCoreContainer().getZkController();
    if (zkController != null && core.getCoreDescriptor().getCloudDescriptor().getReplicaType() == Replica.Type.TLOG) {
      commitWithinSoftCommit = false;
      commitTracker.setOpenSearcher(true);
    }
    ObjectReleaseTracker.track(this);
  }
  
  public DirectUpdateHandler2(SolrCore core, UpdateHandler updateHandler) {
    super(core, updateHandler.getUpdateLog());
    solrCoreState = core.getSolrCoreState();
    
    UpdateHandlerInfo updateHandlerInfo = core.getSolrConfig()
        .getUpdateHandlerInfo();
    int docsUpperBound = updateHandlerInfo.autoCommmitMaxDocs;
    int timeUpperBound = updateHandlerInfo.autoCommmitMaxTime;
    long fileSizeUpperBound = updateHandlerInfo.autoCommitMaxSizeBytes;
    commitTracker = new CommitTracker("Hard", core, docsUpperBound, timeUpperBound, fileSizeUpperBound, updateHandlerInfo.openSearcher, false);
    
    int softCommitDocsUpperBound = updateHandlerInfo.autoSoftCommmitMaxDocs;
    int softCommitTimeUpperBound = updateHandlerInfo.autoSoftCommmitMaxTime;
    softCommitTracker = new CommitTracker("Soft", core, softCommitDocsUpperBound, softCommitTimeUpperBound, NO_FILE_SIZE_UPPER_BOUND_PLACEHOLDER, updateHandlerInfo.openSearcher, true);
    
    commitWithinSoftCommit = updateHandlerInfo.commitWithinSoftCommit;
    indexWriterCloseWaitsForMerges = updateHandlerInfo.indexWriterCloseWaitsForMerges;

    UpdateLog existingLog = updateHandler.getUpdateLog();
    if (this.ulog != null && this.ulog == existingLog) {
      // If we are reusing the existing update log, inform the log that its update handler has changed.
      // We do this as late as possible.
      this.ulog.init(this, core);
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    commitCommands = solrMetricsContext.meter("commits", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> commitTracker.getCommitCount(), true, "autoCommits", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> softCommitTracker.getCommitCount(), true, "softAutoCommits", getCategory().toString(), scope);
    if (commitTracker.getDocsUpperBound() > 0) {
      solrMetricsContext.gauge(() -> commitTracker.getDocsUpperBound(), true, "autoCommitMaxDocs",
          getCategory().toString(), scope);
    }
    if (commitTracker.getTimeUpperBound() > 0) {
      solrMetricsContext.gauge(() -> "" + commitTracker.getTimeUpperBound() + "ms", true, "autoCommitMaxTime",
          getCategory().toString(), scope);
    }
    if (commitTracker.getTLogFileSizeUpperBound() > 0) {
      solrMetricsContext.gauge(() -> commitTracker.getTLogFileSizeUpperBound(), true, "autoCommitMaxSize",
          getCategory().toString(), scope);
    }
    if (softCommitTracker.getDocsUpperBound() > 0) {
      solrMetricsContext.gauge(() -> softCommitTracker.getDocsUpperBound(), true, "softAutoCommitMaxDocs",
          getCategory().toString(), scope);
    }
    if (softCommitTracker.getTimeUpperBound() > 0) {
      solrMetricsContext.gauge(() -> "" + softCommitTracker.getTimeUpperBound() + "ms", true, "softAutoCommitMaxTime",
          getCategory().toString(), scope);
    }
    optimizeCommands = solrMetricsContext.meter("optimizes", getCategory().toString(), scope);
    rollbackCommands = solrMetricsContext.meter("rollbacks", getCategory().toString(), scope);
    splitCommands = solrMetricsContext.meter("splits", getCategory().toString(), scope);
    mergeIndexesCommands = solrMetricsContext.meter("merges", getCategory().toString(), scope);
    expungeDeleteCommands = solrMetricsContext.meter("expungeDeletes", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> numDocsPending.longValue(), true, "docsPending", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> addCommands.longValue(), true, "adds", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> deleteByIdCommands.longValue(), true, "deletesById", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> deleteByQueryCommands.longValue(), true, "deletesByQuery", getCategory().toString(), scope);
    solrMetricsContext.gauge(() -> numErrors.longValue(), true, "errors", getCategory().toString(), scope);

    addCommandsCumulative = solrMetricsContext.meter("cumulativeAdds", getCategory().toString(), scope);
    deleteByIdCommandsCumulative = solrMetricsContext.meter("cumulativeDeletesById", getCategory().toString(), scope);
    deleteByQueryCommandsCumulative = solrMetricsContext.meter("cumulativeDeletesByQuery", getCategory().toString(), scope);
    numErrorsCumulative = solrMetricsContext.meter("cumulativeErrors", getCategory().toString(), scope);
  }

  private void deleteAll() throws IOException {
    if (log.isInfoEnabled()) {
      log.info("{} REMOVING ALL DOCUMENTS FROM INDEX", core.getLogId());
    }
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      iw.get().deleteAll();
    } finally {
      iw.decref();
    }
  }

  protected void rollbackWriter() throws IOException {
    numDocsPending.reset();
    solrCoreState.rollbackIndexWriter(core);
    
  }

  @Override
  public int addDoc(AddUpdateCommand cmd) throws IOException {
    TestInjection.injectDirectUpdateLatch();
    try {
      return addDoc0(cmd);
    } catch (SolrException e) {
      throw e;
    } catch (AlreadyClosedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          String.format(Locale.ROOT, "Server error writing document id %s to the index", cmd.getPrintableId()), e);
    } catch (IllegalArgumentException iae) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "Exception writing document id %s to the index; possible analysis error: "
              + iae.getMessage()
              + (iae.getCause() instanceof BytesRefHash.MaxBytesLengthExceededException ?
              ". Perhaps the document has an indexed string field (solr.StrField) which is too large" : ""),
              cmd.getPrintableId()), iae);
    } catch (RuntimeException t) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "Exception writing document id %s to the index; possible analysis error.",
          cmd.getPrintableId()), t);
    }
  }

  /**
   * This is the implementation of {@link #addDoc(AddUpdateCommand)}. It is factored out to allow an exception
   * handler to decorate RuntimeExceptions with information about the document being handled.
   * @param cmd the command.
   * @return the count.
   */
  private int addDoc0(AddUpdateCommand cmd) throws IOException {
    int rc = -1;

    addCommands.increment();
    addCommandsCumulative.mark();

    // if there is no ID field, don't overwrite
    if (idField == null) {
      cmd.overwrite = false;
    }
    try {
      if ((cmd.getFlags() & UpdateCommand.IGNORE_INDEXWRITER) != 0) {
        if (ulog != null) ulog.add(cmd);
        return 1;
      }

      if (cmd.overwrite) {
        // Check for delete by query commands newer (i.e. reordered). This
        // should always be null on a leader
        List<UpdateLog.DBQ> deletesAfter = null;
        if (ulog != null && cmd.version > 0) {
          deletesAfter = ulog.getDBQNewer(cmd.version);
        }

        if (deletesAfter != null) {
          addAndDelete(cmd, deletesAfter);
        } else {
          doNormalUpdate(cmd);
        }
      } else {
        allowDuplicateUpdate(cmd);
      }

      if ((cmd.getFlags() & UpdateCommand.IGNORE_AUTOCOMMIT) == 0) {
        long currentTlogSize = getCurrentTLogSize();
        if (commitWithinSoftCommit) {
          commitTracker.addedDocument(-1, currentTlogSize);
          softCommitTracker.addedDocument(cmd.commitWithin);
        } else {
          softCommitTracker.addedDocument(-1);
          commitTracker.addedDocument(cmd.commitWithin, currentTlogSize);
        }
      }

      rc = 1;
    } finally {
      if (rc != 1) {
        numErrors.increment();
        numErrorsCumulative.mark();
      } else {
        numDocsPending.increment();
      }
    }

    return rc;
  }

  private void allowDuplicateUpdate(AddUpdateCommand cmd) throws IOException {
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      IndexWriter writer = iw.get();
      Iterable<Document> nestedDocs = cmd.getLuceneDocsIfNested();
      if (nestedDocs != null) {
        writer.addDocuments(nestedDocs);
      } else {
        writer.addDocument(cmd.getLuceneDocument());
      }
      if (ulog != null) ulog.add(cmd);

    } finally {
      iw.decref();
    }

  }

  private void doNormalUpdate(AddUpdateCommand cmd) throws IOException {
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      IndexWriter writer = iw.get();

      updateDocOrDocValues(cmd, writer);

      // Add to the transaction log *after* successfully adding to the
      // index, if there was no error.
      // This ordering ensures that if we log it, it's definitely been
      // added to the the index.
      // This also ensures that if a commit sneaks in-between, that we
      // know everything in a particular
      // log version was definitely committed.
      if (ulog != null) ulog.add(cmd);

    } finally {
      iw.decref();
    }
  }

  private void addAndDelete(AddUpdateCommand cmd, List<UpdateLog.DBQ> deletesAfter) throws IOException {
    // this logic is different enough from doNormalUpdate that it's separate
    log.info("Reordered DBQs detected.  Update={} DBQs={}", cmd, deletesAfter);
    List<Query> dbqList = new ArrayList<>(deletesAfter.size());
    for (UpdateLog.DBQ dbq : deletesAfter) {
      try {
        DeleteUpdateCommand tmpDel = new DeleteUpdateCommand(cmd.req);
        tmpDel.query = dbq.q;
        tmpDel.version = -dbq.version;
        dbqList.add(getQuery(tmpDel));
      } catch (Exception e) {
        log.error("Exception parsing reordered query : {}", dbq, e);
      }
    }

    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      IndexWriter writer = iw.get();

      // see comment in deleteByQuery
      synchronized (solrCoreState.getUpdateLock()) {
        updateDocOrDocValues(cmd, writer);

        if (cmd.isInPlaceUpdate() && ulog != null) {
          ulog.openRealtimeSearcher(); // This is needed due to LUCENE-7344.
        }
        for (Query q : dbqList) {
          writer.deleteDocuments(new DeleteByQueryWrapper(q, core.getLatestSchema()));
        }
        if (ulog != null) ulog.add(cmd, true); // this needs to be protected by update lock
      }
    } finally {
      iw.decref();
    }

  }

  private void updateDeleteTrackers(DeleteUpdateCommand cmd) {
    if ((cmd.getFlags() & UpdateCommand.IGNORE_AUTOCOMMIT) == 0) {
      if (commitWithinSoftCommit) {
        softCommitTracker.deletedDocument(cmd.commitWithin);
      } else {
        commitTracker.deletedDocument(cmd.commitWithin);
      }
      
      if (commitTracker.getTimeUpperBound() > 0) {
        commitTracker.scheduleCommitWithin(commitTracker.getTimeUpperBound());
      }

      long currentTlogSize = getCurrentTLogSize();
      commitTracker.scheduleMaxSizeTriggeredCommitIfNeeded(currentTlogSize);
      
      if (softCommitTracker.getTimeUpperBound() > 0) {
        softCommitTracker.scheduleCommitWithin(softCommitTracker
            .getTimeUpperBound());
      }
    }
  }

  // we don't return the number of docs deleted because it's not always possible to quickly know that info.
  @Override
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    TestInjection.injectDirectUpdateLatch();
    deleteByIdCommands.increment();
    deleteByIdCommandsCumulative.mark();

    if ((cmd.getFlags() & UpdateCommand.IGNORE_INDEXWRITER) != 0 ) {
      if (ulog != null) ulog.delete(cmd);
      return;
    }

    Term deleteTerm = getIdTerm(cmd.getIndexedId(), false);
    // SolrCore.verbose("deleteDocuments",deleteTerm,writer);
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      iw.get().deleteDocuments(deleteTerm);
    } finally {
      iw.decref();
    }
    // SolrCore.verbose("deleteDocuments",deleteTerm,"DONE");

    if (ulog != null) ulog.delete(cmd);

    updateDeleteTrackers(cmd);
  }


  public void clearIndex() throws IOException {
    deleteAll();
    if (ulog != null) {
      ulog.deleteAll();
    }
  }


  private Query getQuery(DeleteUpdateCommand cmd) {
    Query q;
    try {
      // move this higher in the stack?
      QParser parser = QParser.getParser(cmd.getQuery(), cmd.req);
      q = parser.getQuery();
      q = QueryUtils.makeQueryable(q);

      // Make sure not to delete newer versions
      if (ulog != null && cmd.getVersion() != 0 && cmd.getVersion() != -Long.MAX_VALUE) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(q, Occur.MUST);
        SchemaField sf = ulog.getVersionInfo().getVersionField();
        ValueSource vs = sf.getType().getValueSource(sf, null);
        ValueSourceRangeFilter filt = new ValueSourceRangeFilter(vs, Long.toString(Math.abs(cmd.getVersion())), null, true, true);
        FunctionRangeQuery range = new FunctionRangeQuery(filt);
        bq.add(range, Occur.MUST_NOT);  // formulated in the "MUST_NOT" sense so we can delete docs w/o a version (some tests depend on this...)
        q = bq.build();
      }

      return q;

    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }


  // we don't return the number of docs deleted because it's not always possible to quickly know that info.
  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    TestInjection.injectDirectUpdateLatch();
    deleteByQueryCommands.increment();
    deleteByQueryCommandsCumulative.mark();
    boolean madeIt=false;
    try {
      if ((cmd.getFlags() & UpdateCommand.IGNORE_INDEXWRITER) != 0) {
        if (ulog != null) ulog.deleteByQuery(cmd);
        madeIt = true;
        return;
      }
      Query q = getQuery(cmd);
      
      boolean delAll = MatchAllDocsQuery.class == q.getClass();

      // currently for testing purposes.  Do a delete of complete index w/o worrying about versions, don't log, clean up most state in update log, etc
      if (delAll && cmd.getVersion() == -Long.MAX_VALUE) {
        synchronized (solrCoreState.getUpdateLock()) {
          deleteAll();
          ulog.deleteAll();
          return;
        }
      }

      //
      // synchronized to prevent deleteByQuery from running during the "open new searcher"
      // part of a commit.  DBQ needs to signal that a fresh reader will be needed for
      // a realtime view of the index.  When a new searcher is opened after a DBQ, that
      // flag can be cleared.  If those thing happen concurrently, it's not thread safe.
      // Also, ulog.deleteByQuery clears caches and is thus not safe to be called between
      // preSoftCommit/postSoftCommit and thus we use the updateLock to prevent this (just
      // as we use around ulog.preCommit... also see comments in ulog.postSoftCommit)
      //
      synchronized (solrCoreState.getUpdateLock()) {

        // We are reopening a searcher before applying the deletes to overcome LUCENE-7344.
        // Once LUCENE-7344 is resolved, we can consider removing this.
        if (ulog != null) ulog.openRealtimeSearcher();

        if (delAll) {
          deleteAll();
        } else {
          RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
          try {
            iw.get().deleteDocuments(new DeleteByQueryWrapper(q, core.getLatestSchema()));
          } finally {
            iw.decref();
          }
        }

        if (ulog != null) ulog.deleteByQuery(cmd);  // this needs to be protected by the update lock
      }

      madeIt = true;

      updateDeleteTrackers(cmd);

    } finally {
      if (!madeIt) {
        numErrors.increment();
        numErrorsCumulative.mark();
      }
    }
  }

  @Override
  public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
    TestInjection.injectDirectUpdateLatch();
    mergeIndexesCommands.mark();
    int rc;

    log.info("start {}", cmd);
    
    List<DirectoryReader> readers = cmd.readers;
    if (readers != null && readers.size() > 0) {
      List<CodecReader> mergeReaders = new ArrayList<>();
      for (DirectoryReader reader : readers) {
        for (LeafReaderContext leaf : reader.leaves()) {
          mergeReaders.add(SlowCodecReaderWrapper.wrap(leaf.reader()));
        }
      }
      RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
      try {
        iw.get().addIndexes(mergeReaders.toArray(new CodecReader[mergeReaders.size()]));
      } finally {
        iw.decref();
      }
      rc = 1;
    } else {
      rc = 0;
    }
    log.info("end_mergeIndexes");

    // TODO: consider soft commit issues
    if (rc == 1 && commitTracker.getTimeUpperBound() > 0) {
      commitTracker.scheduleCommitWithin(commitTracker.getTimeUpperBound());
    } else if (rc == 1 && softCommitTracker.getTimeUpperBound() > 0) {
      softCommitTracker.scheduleCommitWithin(softCommitTracker.getTimeUpperBound());
    }

    return rc;
  }
  
  public void prepareCommit(CommitUpdateCommand cmd) throws IOException {

    boolean error=true;

    try {
      log.debug("start {}", cmd);
      RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
      try {
        SolrIndexWriter.setCommitData(iw.get(), cmd.getVersion());
        iw.get().prepareCommit();
      } finally {
        iw.decref();
      }

      log.debug("end_prepareCommit");

      error=false;
    }
    finally {
      if (error) {
        numErrors.increment();
        numErrorsCumulative.mark();
      }
    }
  }

  @Override
  public void commit(CommitUpdateCommand cmd) throws IOException {
    TestInjection.injectDirectUpdateLatch();
    if (cmd.prepareCommit) {
      prepareCommit(cmd);
      return;
    }

    if (cmd.optimize) {
      optimizeCommands.mark();
    } else {
      commitCommands.mark();
      if (cmd.expungeDeletes) expungeDeleteCommands.mark();
    }

    @SuppressWarnings({"rawtypes"})
    Future[] waitSearcher = null;
    if (cmd.waitSearcher) {
      waitSearcher = new Future[1];
    }

    boolean error=true;
    try {
      // only allow one hard commit to proceed at once
      if (!cmd.softCommit) {
        solrCoreState.getCommitLock().lock();
      }

      log.debug("start {}", cmd);

      // We must cancel pending commits *before* we actually execute the commit.

      if (cmd.openSearcher) {
        // we can cancel any pending soft commits if this commit will open a new searcher
        softCommitTracker.cancelPendingCommit();
      }
      if (!cmd.softCommit && (cmd.openSearcher || !commitTracker.getOpenSearcher())) {
        // cancel a pending hard commit if this commit is of equal or greater "strength"...
        // If the autoCommit has openSearcher=true, then this commit must have openSearcher=true
        // to cancel.
         commitTracker.cancelPendingCommit();
      }

      RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
      try {
        IndexWriter writer = iw.get();
        if (cmd.optimize) {
          writer.forceMerge(cmd.maxOptimizeSegments);
        } else if (cmd.expungeDeletes) {
          writer.forceMergeDeletes();
        }
        
        if (!cmd.softCommit) {
          synchronized (solrCoreState.getUpdateLock()) { // sync is currently needed to prevent preCommit
                                // from being called between preSoft and
                                // postSoft... see postSoft comments.
            if (ulog != null) ulog.preCommit(cmd);
          }
          
          // SolrCore.verbose("writer.commit() start writer=",writer);

          if (writer.hasUncommittedChanges()) {
            SolrIndexWriter.setCommitData(writer, cmd.getVersion());
            writer.commit();
          } else {
            log.info("No uncommitted changes. Skipping IW.commit.");
          }

          // SolrCore.verbose("writer.commit() end");
          numDocsPending.reset();
          callPostCommitCallbacks();
        }
      } finally {
        iw.decref();
      }


      if (cmd.optimize) {
        callPostOptimizeCallbacks();
      }


      if (cmd.softCommit) {
        // ulog.preSoftCommit();
        synchronized (solrCoreState.getUpdateLock()) {
          if (ulog != null) ulog.preSoftCommit(cmd);
          core.getSearcher(true, false, waitSearcher, true);
          if (ulog != null) ulog.postSoftCommit(cmd);
        }
        callPostSoftCommitCallbacks();
      } else {
        synchronized (solrCoreState.getUpdateLock()) {
          if (ulog != null) ulog.preSoftCommit(cmd);
          if (cmd.openSearcher) {
            core.getSearcher(true, false, waitSearcher);
          } else {
            // force open a new realtime searcher so realtime-get and versioning code can see the latest
            RefCounted<SolrIndexSearcher> searchHolder = core.openNewSearcher(true, true);
            searchHolder.decref();
          }
          if (ulog != null) ulog.postSoftCommit(cmd);
        }
        if (ulog != null) ulog.postCommit(cmd); // postCommit currently means new searcher has
                              // also been opened
      }

      // reset commit tracking

      if (cmd.softCommit) {
        softCommitTracker.didCommit();
      } else {
        commitTracker.didCommit();
      }
      
      log.debug("end_commit_flush");

      error=false;
    }
    finally {
      if (!cmd.softCommit) {
        solrCoreState.getCommitLock().unlock();
      }

      addCommands.reset();
      deleteByIdCommands.reset();
      deleteByQueryCommands.reset();
      if (error) {
        numErrors.increment();
        numErrorsCumulative.mark();
      }
    }

    // if we are supposed to wait for the searcher to be registered, then we should do it
    // outside any synchronized block so that other update operations can proceed.
    if (waitSearcher!=null && waitSearcher[0] != null) {
       try {
        waitSearcher[0].get();
      } catch (InterruptedException | ExecutionException e) {
         ParWork.propegateInterrupt(e);
      }
    }
  }

  @Override
  public void newIndexWriter(boolean rollback) throws IOException {
    solrCoreState.newIndexWriter(core, rollback);
  }
  
  /**
   * @since Solr 1.4
   */
  @Override
  public void rollback(RollbackUpdateCommand cmd) throws IOException {
    TestInjection.injectDirectUpdateLatch();
    if (core.getCoreContainer().isZooKeeperAware()) {
      throw new UnsupportedOperationException("Rollback is currently not supported in SolrCloud mode. (SOLR-4895)");
    }

    rollbackCommands.mark();

    boolean error=true;

    try {
      log.info("start {}", cmd);

      rollbackWriter();

      //callPostRollbackCallbacks();

      // reset commit tracking
      commitTracker.didRollback();
      softCommitTracker.didRollback();
      
      log.info("end_rollback");

      error=false;
    }
    finally {
      addCommandsCumulative.mark(-addCommands.sumThenReset());
      deleteByIdCommandsCumulative.mark(-deleteByIdCommands.sumThenReset());
      deleteByQueryCommandsCumulative.mark(-deleteByQueryCommands.sumThenReset());
      if (error) {
        numErrors.increment();
        numErrorsCumulative.mark();
      }
    }
  }

  @Override
  public UpdateLog getUpdateLog() {
    return ulog;
  }

  @Override
  public void close() throws IOException {
    log.debug("closing {}", this);
    try (ParWork closer = new ParWork(this, true)) {
      closer.collect(commitTracker);
      closer.collect(softCommitTracker);
    }
    super.close();
    numDocsPending.reset();
    ObjectReleaseTracker.release(this);
  }

  // IndexWriterCloser interface method - called from solrCoreState.decref(this)
  @Override
  public void closeWriter(IndexWriter writer) throws IOException {
    log.trace("closeWriter({}): ulog={}", writer, ulog);
    
    assert TestInjection.injectNonGracefullClose(core.getCoreContainer());

    boolean clearRequestInfo = false;

    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
    SolrQueryResponse rsp = new SolrQueryResponse();
    if (SolrRequestInfo.getRequestInfo() == null) {
      clearRequestInfo = true;
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp)); // important for debugging
    }
    try {

      if (TestInjection.injectSkipIndexWriterCommitOnClose(writer) || Boolean.getBoolean("solr.skipCommitOnClose")) {
        // if this TestInjection triggers, we do some simple rollback()
        // (which closes the underlying IndexWriter) and then return immediately
        log.warn("Skipping commit for IndexWriter.close() due to TestInjection or system property");
        if (writer != null) {
          writer.rollback();
        }
        // we shouldn't close the transaction logs either, but leaving them open
        // means we can't delete them on windows (needed for tests)
        if (ulog != null) ulog.close(false);

        return;
      }
      
      // do a commit before we quit?
      boolean tryToCommit = writer != null && ulog != null && ulog.hasUncommittedChanges()
          && ulog.getState() == UpdateLog.State.ACTIVE;

      // be tactical with this lock! closing the updatelog can deadlock when it tries to commit
      solrCoreState.getCommitLock().lock();
      try {
        try {
          if (log.isInfoEnabled()) {
            log.info("Committing on IndexWriter.close() {}.",
                (tryToCommit ? "" : " ... SKIPPED (unnecessary)"));
          }
          if (tryToCommit) {
            CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
            cmd.openSearcher = false;
            cmd.waitSearcher = false;
            cmd.softCommit = false;

            // TODO: keep other commit callbacks from being called?
            // this.commit(cmd); // too many test failures using this method... is it because of callbacks?

            synchronized (solrCoreState.getUpdateLock()) {
              ulog.preCommit(cmd);
            }

            // todo: refactor this shared code (or figure out why a real CommitUpdateCommand can't be used)
            SolrIndexWriter.setCommitData(writer, cmd.getVersion());
            writer.commit();

            synchronized (solrCoreState.getUpdateLock()) {
              ulog.postCommit(cmd);
            }
          }
        } catch (Throwable th) {
          log.error("Error in final commit", th);
          if (th instanceof OutOfMemoryError) {
            throw (OutOfMemoryError) th;
          }
        }

      } finally {
        solrCoreState.getCommitLock().unlock();

      }
    } finally {
      if (clearRequestInfo) SolrRequestInfo.clearRequestInfo();
    }
    // we went through the normal process to commit, so we don't have to artificially
    // cap any ulog files.
    try {
      if (ulog != null) ulog.close(false);
    } catch (Throwable th) {
      log.error("Error closing log files", th);
      if (th instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) th;
      }
    }

    if (writer != null) {
      writer.close();
    }

  }

  @Override
  public void split(SplitIndexCommand cmd) throws IOException {
    commit(new CommitUpdateCommand(cmd.req, false));
    SolrIndexSplitter splitter = new SolrIndexSplitter(cmd);
    splitCommands.mark();
    NamedList<Object> results = new NamedList<>();
    try {
      splitter.split(results);
      cmd.rsp.addResponse(results);
    } catch (IOException e) {
      numErrors.increment();
      numErrorsCumulative.mark();
      throw e;
    }
  }

  /**
   * Calls either {@link IndexWriter#updateDocValues} or <code>IndexWriter#updateDocument</code>(s) as
   * needed based on {@link AddUpdateCommand#isInPlaceUpdate}.
   * <p>
   * If the this is an UPDATE_INPLACE cmd, then all fields included in 
   * {@link AddUpdateCommand#getLuceneDocument} must either be the uniqueKey field, or be DocValue 
   * only fields.
   * </p>
   *
   * @param cmd - cmd apply to IndexWriter
   * @param writer - IndexWriter to use
   */
  private void updateDocOrDocValues(AddUpdateCommand cmd, IndexWriter writer) throws IOException {
    assert idField != null; // this code path requires an idField in order to potentially replace a doc
    boolean hasUpdateTerm = cmd.updateTerm != null; // AKA dedupe

    if (cmd.isInPlaceUpdate()) {
      if (hasUpdateTerm) {
        throw new IllegalStateException("cmd.updateTerm/dedupe is not compatible with in-place updates");
      }
      // we don't support the solrInputDoc with nested child docs either but we'll throw an exception if attempted

      Term updateTerm = new Term(idField.getName(), cmd.getIndexedId());
      Document luceneDocument = cmd.getLuceneDocument();

      final List<IndexableField> origDocFields = luceneDocument.getFields();
      final List<Field> fieldsToUpdate = new ArrayList<>(origDocFields.size());
      for (IndexableField field : origDocFields) {
        if (! field.name().equals(updateTerm.field()) ) {
          fieldsToUpdate.add((Field)field);
        }
      }
      log.debug("updateDocValues({})", cmd);
      writer.updateDocValues(updateTerm, fieldsToUpdate.toArray(new Field[fieldsToUpdate.size()]));

    } else { // more normal path

      Iterable<Document> nestedDocs = cmd.getLuceneDocsIfNested();
      boolean isNested = nestedDocs != null; // AKA nested child docs
      Term idTerm = getIdTerm(isNested? new BytesRef(cmd.getRootIdUsingRouteParam()): cmd.getIndexedId(), isNested);
      Term updateTerm = hasUpdateTerm ? cmd.updateTerm : idTerm;
      if (isNested) {
        log.debug("updateDocuments({})", cmd);
        writer.updateDocuments(updateTerm, nestedDocs);
      } else {
        Document luceneDocument = cmd.getLuceneDocument();
        log.debug("updateDocument({})", cmd);
        writer.updateDocument(updateTerm, luceneDocument);
      }

      // If hasUpdateTerm, then delete any existing documents with the same ID other than the one added above
      //   (used in near-duplicate replacement)
      if (hasUpdateTerm) { // rare
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(updateTerm), Occur.MUST_NOT); //don't want the one we added above (will be unique)
        bq.add(new TermQuery(idTerm), Occur.MUST); // same ID
        writer.deleteDocuments(new DeleteByQueryWrapper(bq.build(), core.getLatestSchema()));
      }
    }
  }

  private Term getIdTerm(BytesRef termVal, boolean isNested) {
    boolean useRootId = isNested || core.getLatestSchema().isUsableForChildDocs();
    return new Term(useRootId ? IndexSchema.ROOT_FIELD_NAME : idField.getName(), termVal);
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  @Override
  public String getName() {
    return DirectUpdateHandler2.class.getName();
  }

  @Override
  public String getDescription() {
    return "Update handler that efficiently directly updates the on-disk main lucene index";
  }

  @Override
  public SolrCoreState getSolrCoreState() {
    return solrCoreState;
  }

  private long getCurrentTLogSize() {
    UpdateLog fulog = ulog;
    return fulog != null && fulog.hasUncommittedChanges() ? fulog.getCurrentLogSizeFromStream() : -1;
  }

  // allow access for tests
  public CommitTracker getCommitTracker() {
    return commitTracker;
  }

  // allow access for tests
  public CommitTracker getSoftCommitTracker() {
    return softCommitTracker;
  }

}
