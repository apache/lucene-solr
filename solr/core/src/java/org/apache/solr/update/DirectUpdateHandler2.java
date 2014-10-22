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

/**
 */

package org.apache.solr.update;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrConfig.UpdateHandlerInfo;
import org.apache.solr.core.SolrCore;
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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <code>DirectUpdateHandler2</code> implements an UpdateHandler where documents are added
 * directly to the main Lucene index as opposed to adding to a separate smaller index.
 * <p>
 * TODO: add soft commitWithin support
 */
public class DirectUpdateHandler2 extends UpdateHandler implements SolrCoreState.IndexWriterCloser {
  protected final SolrCoreState solrCoreState;

  // stats
  AtomicLong addCommands = new AtomicLong();
  AtomicLong addCommandsCumulative = new AtomicLong();
  AtomicLong deleteByIdCommands= new AtomicLong();
  AtomicLong deleteByIdCommandsCumulative= new AtomicLong();
  AtomicLong deleteByQueryCommands= new AtomicLong();
  AtomicLong deleteByQueryCommandsCumulative= new AtomicLong();
  AtomicLong expungeDeleteCommands = new AtomicLong();
  AtomicLong mergeIndexesCommands = new AtomicLong();
  AtomicLong commitCommands= new AtomicLong();
  AtomicLong optimizeCommands= new AtomicLong();
  AtomicLong rollbackCommands= new AtomicLong();
  AtomicLong numDocsPending= new AtomicLong();
  AtomicLong numErrors = new AtomicLong();
  AtomicLong numErrorsCumulative = new AtomicLong();

  // tracks when auto-commit should occur
  protected final CommitTracker commitTracker;
  protected final CommitTracker softCommitTracker;
  
  protected boolean commitWithinSoftCommit;

  protected boolean indexWriterCloseWaitsForMerges;
  
  public DirectUpdateHandler2(SolrCore core) {
    super(core);
   
    solrCoreState = core.getSolrCoreState();
    
    UpdateHandlerInfo updateHandlerInfo = core.getSolrConfig()
        .getUpdateHandlerInfo();
    int docsUpperBound = updateHandlerInfo.autoCommmitMaxDocs; // getInt("updateHandler/autoCommit/maxDocs", -1);
    int timeUpperBound = updateHandlerInfo.autoCommmitMaxTime; // getInt("updateHandler/autoCommit/maxTime", -1);
    commitTracker = new CommitTracker("Hard", core, docsUpperBound, timeUpperBound, updateHandlerInfo.openSearcher, false);
    
    int softCommitDocsUpperBound = updateHandlerInfo.autoSoftCommmitMaxDocs; // getInt("updateHandler/autoSoftCommit/maxDocs", -1);
    int softCommitTimeUpperBound = updateHandlerInfo.autoSoftCommmitMaxTime; // getInt("updateHandler/autoSoftCommit/maxTime", -1);
    softCommitTracker = new CommitTracker("Soft", core, softCommitDocsUpperBound, softCommitTimeUpperBound, true, true);
    
    commitWithinSoftCommit = updateHandlerInfo.commitWithinSoftCommit;
    indexWriterCloseWaitsForMerges = updateHandlerInfo.indexWriterCloseWaitsForMerges;


  }
  
  public DirectUpdateHandler2(SolrCore core, UpdateHandler updateHandler) {
    super(core, updateHandler.getUpdateLog());
    solrCoreState = core.getSolrCoreState();
    
    UpdateHandlerInfo updateHandlerInfo = core.getSolrConfig()
        .getUpdateHandlerInfo();
    int docsUpperBound = updateHandlerInfo.autoCommmitMaxDocs; // getInt("updateHandler/autoCommit/maxDocs", -1);
    int timeUpperBound = updateHandlerInfo.autoCommmitMaxTime; // getInt("updateHandler/autoCommit/maxTime", -1);
    commitTracker = new CommitTracker("Hard", core, docsUpperBound, timeUpperBound, updateHandlerInfo.openSearcher, false);
    
    int softCommitDocsUpperBound = updateHandlerInfo.autoSoftCommmitMaxDocs; // getInt("updateHandler/autoSoftCommit/maxDocs", -1);
    int softCommitTimeUpperBound = updateHandlerInfo.autoSoftCommmitMaxTime; // getInt("updateHandler/autoSoftCommit/maxTime", -1);
    softCommitTracker = new CommitTracker("Soft", core, softCommitDocsUpperBound, softCommitTimeUpperBound, updateHandlerInfo.openSearcher, true);
    
    commitWithinSoftCommit = updateHandlerInfo.commitWithinSoftCommit;
    indexWriterCloseWaitsForMerges = updateHandlerInfo.indexWriterCloseWaitsForMerges;

    UpdateLog existingLog = updateHandler.getUpdateLog();
    if (this.ulog != null && this.ulog == existingLog) {
      // If we are reusing the existing update log, inform the log that it's update handler has changed.
      // We do this as late as possible.
      this.ulog.init(this, core);
    }
  }

  private void deleteAll() throws IOException {
    SolrCore.log.info(core.getLogId()+"REMOVING ALL DOCUMENTS FROM INDEX");
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      iw.get().deleteAll();
    } finally {
      iw.decref();
    }
  }

  protected void rollbackWriter() throws IOException {
    numDocsPending.set(0);
    solrCoreState.rollbackIndexWriter(core);
    
  }

  @Override
  public int addDoc(AddUpdateCommand cmd) throws IOException {
    try {
      return addDoc0(cmd);
    } catch (SolrException e) {
      throw e;
    } catch (RuntimeException t) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          String.format(Locale.ROOT, "Exception writing document id %s to the index; possible analysis error.",
          cmd.getPrintableId()), t);
    }
  }

  /**
   * This is the implementation of {@link #addDoc0(AddUpdateCommand)}. It is factored out to allow an exception
   * handler to decorate RuntimeExceptions with information about the document being handled.
   * @param cmd the command.
   * @return the count.
   */
  private int addDoc0(AddUpdateCommand cmd) throws IOException {
    int rc = -1;
    RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
    try {
      IndexWriter writer = iw.get();
      addCommands.incrementAndGet();
      addCommandsCumulative.incrementAndGet();
      
      // if there is no ID field, don't overwrite
      if (idField == null) {
        cmd.overwrite = false;
      }
      
      try {
        IndexSchema schema = cmd.getReq().getSchema();
        
        if (cmd.overwrite) {
          
          // Check for delete by query commands newer (i.e. reordered). This
          // should always be null on a leader
          List<UpdateLog.DBQ> deletesAfter = null;
          if (ulog != null && cmd.version > 0) {
            deletesAfter = ulog.getDBQNewer(cmd.version);
          }
          
          if (deletesAfter != null) {
            log.info("Reordered DBQs detected.  Update=" + cmd + " DBQs="
                + deletesAfter);
            List<Query> dbqList = new ArrayList<>(deletesAfter.size());
            for (UpdateLog.DBQ dbq : deletesAfter) {
              try {
                DeleteUpdateCommand tmpDel = new DeleteUpdateCommand(cmd.req);
                tmpDel.query = dbq.q;
                tmpDel.version = -dbq.version;
                dbqList.add(getQuery(tmpDel));
              } catch (Exception e) {
                log.error("Exception parsing reordered query : " + dbq, e);
              }
            }
            
            addAndDelete(cmd, dbqList);
          } else {
            // normal update
            
            Term updateTerm;
            Term idTerm = new Term(cmd.isBlock() ? "_root_" : idField.getName(), cmd.getIndexedId());
            boolean del = false;
            if (cmd.updateTerm == null) {
              updateTerm = idTerm;
            } else {
              // this is only used by the dedup update processor
              del = true;
              updateTerm = cmd.updateTerm;
            }

            if (cmd.isBlock()) {
              writer.updateDocuments(updateTerm, cmd, schema.getIndexAnalyzer());
            } else {
              Document luceneDocument = cmd.getLuceneDocument();
              // SolrCore.verbose("updateDocument",updateTerm,luceneDocument,writer);
              writer.updateDocument(updateTerm, luceneDocument, schema.getIndexAnalyzer());
            }
            // SolrCore.verbose("updateDocument",updateTerm,"DONE");
            
            if (del) { // ensure id remains unique
              BooleanQuery bq = new BooleanQuery();
              bq.add(new BooleanClause(new TermQuery(updateTerm),
                  Occur.MUST_NOT));
              bq.add(new BooleanClause(new TermQuery(idTerm), Occur.MUST));
              writer.deleteDocuments(new DeleteByQueryWrapper(bq, core.getLatestSchema()));
            }
            
            // Add to the transaction log *after* successfully adding to the
            // index, if there was no error.
            // This ordering ensures that if we log it, it's definitely been
            // added to the the index.
            // This also ensures that if a commit sneaks in-between, that we
            // know everything in a particular
            // log version was definitely committed.
            if (ulog != null) ulog.add(cmd);
          }
          
        } else {
          // allow duplicates
          if (cmd.isBlock()) {
            writer.addDocuments(cmd, schema.getIndexAnalyzer());
          } else {
            writer.addDocument(cmd.getLuceneDocument(), schema.getIndexAnalyzer());
          }

          if (ulog != null) ulog.add(cmd);
        }
        
        if ((cmd.getFlags() & UpdateCommand.IGNORE_AUTOCOMMIT) == 0) {
          if (commitWithinSoftCommit) {
            commitTracker.addedDocument(-1);
            softCommitTracker.addedDocument(cmd.commitWithin);
          } else {
            softCommitTracker.addedDocument(-1);
            commitTracker.addedDocument(cmd.commitWithin);
          }
        }
        
        rc = 1;
      } finally {
        if (rc != 1) {
          numErrors.incrementAndGet();
          numErrorsCumulative.incrementAndGet();
        } else {
          numDocsPending.incrementAndGet();
        }
      }
      
    } finally {
      iw.decref();
    }
    
    return rc;
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
      
      if (softCommitTracker.getTimeUpperBound() > 0) {
        softCommitTracker.scheduleCommitWithin(softCommitTracker
            .getTimeUpperBound());
      }
    }
  }

  // we don't return the number of docs deleted because it's not always possible to quickly know that info.
  @Override
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    deleteByIdCommands.incrementAndGet();
    deleteByIdCommandsCumulative.incrementAndGet();

    Term deleteTerm = new Term(idField.getName(), cmd.getIndexedId());
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
      QParser parser = QParser.getParser(cmd.getQuery(), "lucene", cmd.req);
      q = parser.getQuery();
      q = QueryUtils.makeQueryable(q);

      // Make sure not to delete newer versions
      if (ulog != null && cmd.getVersion() != 0 && cmd.getVersion() != -Long.MAX_VALUE) {
        BooleanQuery bq = new BooleanQuery();
        bq.add(q, Occur.MUST);
        SchemaField sf = ulog.getVersionInfo().getVersionField();
        ValueSource vs = sf.getType().getValueSource(sf, null);
        ValueSourceRangeFilter filt = new ValueSourceRangeFilter(vs, null, Long.toString(Math.abs(cmd.getVersion())), true, true);
        FunctionRangeQuery range = new FunctionRangeQuery(filt);
        bq.add(range, Occur.MUST);
        q = bq;
      }

      return q;

    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }


  // we don't return the number of docs deleted because it's not always possible to quickly know that info.
  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    deleteByQueryCommands.incrementAndGet();
    deleteByQueryCommandsCumulative.incrementAndGet();
    boolean madeIt=false;
    try {
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
      //
      synchronized (solrCoreState.getUpdateLock()) {
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

        if (ulog != null) ulog.deleteByQuery(cmd);
      }

      madeIt = true;

      updateDeleteTrackers(cmd);

    } finally {
      if (!madeIt) {
        numErrors.incrementAndGet();
        numErrorsCumulative.incrementAndGet();
      }
    }
  }



  /** Add a document execute the deletes as atomically as possible */
  private void addAndDelete(AddUpdateCommand cmd, List<Query> dbqList)
      throws IOException {
    Document luceneDocument = cmd.getLuceneDocument();
    Term idTerm = new Term(idField.getName(), cmd.getIndexedId());
    
    // see comment in deleteByQuery
    synchronized (solrCoreState.getUpdateLock()) {
      RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
      try {
        IndexWriter writer = iw.get();
        writer.updateDocument(idTerm, luceneDocument, cmd.getReq().getSchema()
            .getIndexAnalyzer());
        
        for (Query q : dbqList) {
          writer.deleteDocuments(new DeleteByQueryWrapper(q, core.getLatestSchema()));
        }
      } finally {
        iw.decref();
      }
      
      if (ulog != null) ulog.add(cmd, true);
    }
    
  }




  @Override
  public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
    mergeIndexesCommands.incrementAndGet();
    int rc;

    log.info("start " + cmd);
    
    List<DirectoryReader> readers = cmd.readers;
    if (readers != null && readers.size() > 0) {
      RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
      try {
        iw.get().addIndexes(readers.toArray(new IndexReader[readers.size()]));
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
      log.info("start "+cmd);
      RefCounted<IndexWriter> iw = solrCoreState.getIndexWriter(core);
      try {
        final Map<String,String> commitData = new HashMap<>();
        commitData.put(SolrIndexWriter.COMMIT_TIME_MSEC_KEY,
            String.valueOf(System.currentTimeMillis()));
        iw.get().setCommitData(commitData);
        iw.get().prepareCommit();
      } finally {
        iw.decref();
      }

      log.info("end_prepareCommit");

      error=false;
    }
    finally {
      if (error) numErrors.incrementAndGet();
    }
  }

  @Override
  public void commit(CommitUpdateCommand cmd) throws IOException {
    if (cmd.prepareCommit) {
      prepareCommit(cmd);
      return;
    }

    if (cmd.optimize) {
      optimizeCommands.incrementAndGet();
    } else {
      commitCommands.incrementAndGet();
      if (cmd.expungeDeletes) expungeDeleteCommands.incrementAndGet();
    }

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

      log.info("start "+cmd);

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
            final Map<String,String> commitData = new HashMap<>();
            commitData.put(SolrIndexWriter.COMMIT_TIME_MSEC_KEY,
                String.valueOf(System.currentTimeMillis()));
            writer.setCommitData(commitData);
            writer.commit();
          } else {
            log.info("No uncommitted changes. Skipping IW.commit.");
          }

          // SolrCore.verbose("writer.commit() end");
          numDocsPending.set(0);
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
      
      log.info("end_commit_flush");

      error=false;
    }
    finally {
      if (!cmd.softCommit) {
        solrCoreState.getCommitLock().unlock();
      }

      addCommands.set(0);
      deleteByIdCommands.set(0);
      deleteByQueryCommands.set(0);
      if (error) numErrors.incrementAndGet();
    }

    // if we are supposed to wait for the searcher to be registered, then we should do it
    // outside any synchronized block so that other update operations can proceed.
    if (waitSearcher!=null && waitSearcher[0] != null) {
       try {
        waitSearcher[0].get();
      } catch (InterruptedException e) {
        SolrException.log(log,e);
      } catch (ExecutionException e) {
        SolrException.log(log,e);
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
    if (core.getCoreDescriptor().getCoreContainer().isZooKeeperAware()) {
      throw new UnsupportedOperationException("Rollback is currently not supported in SolrCloud mode. (SOLR-4895)");
    }

    rollbackCommands.incrementAndGet();

    boolean error=true;

    try {
      log.info("start "+cmd);

      rollbackWriter();

      //callPostRollbackCallbacks();

      // reset commit tracking
      commitTracker.didRollback();
      softCommitTracker.didRollback();
      
      log.info("end_rollback");

      error=false;
    }
    finally {
      addCommandsCumulative.set(
          addCommandsCumulative.get() - addCommands.getAndSet( 0 ) );
      deleteByIdCommandsCumulative.set(
          deleteByIdCommandsCumulative.get() - deleteByIdCommands.getAndSet( 0 ) );
      deleteByQueryCommandsCumulative.set(
          deleteByQueryCommandsCumulative.get() - deleteByQueryCommands.getAndSet( 0 ) );
      if (error) numErrors.incrementAndGet();
    }
  }

  @Override
  public UpdateLog getUpdateLog() {
    return ulog;
  }

  @Override
  public void close() throws IOException {
    log.info("closing " + this);
    
    commitTracker.close();
    softCommitTracker.close();

    numDocsPending.set(0);
  }


  public static boolean commitOnClose = true;  // TODO: make this a real config option?

  // IndexWriterCloser interface method - called from solrCoreState.decref(this)
  @Override
  public void closeWriter(IndexWriter writer) throws IOException {
    boolean clearRequestInfo = false;
    solrCoreState.getCommitLock().lock();
    try {
      SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
      SolrQueryResponse rsp = new SolrQueryResponse();
      if (SolrRequestInfo.getRequestInfo() == null) {
        clearRequestInfo = true;
        SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));  // important for debugging
      }


      if (!commitOnClose) {
        if (writer != null) {
          writer.rollback();
        }

        // we shouldn't close the transaction logs either, but leaving them open
        // means we can't delete them on windows (needed for tests)
        if (ulog != null) ulog.close(false);

        return;
      }

      // do a commit before we quit?     
      boolean tryToCommit = writer != null && ulog != null && ulog.hasUncommittedChanges() && ulog.getState() == UpdateLog.State.ACTIVE;

      try {
        if (tryToCommit) {
          log.info("Committing on IndexWriter close.");
          CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
          cmd.openSearcher = false;
          cmd.waitSearcher = false;
          cmd.softCommit = false;

          // TODO: keep other commit callbacks from being called?
         //  this.commit(cmd);        // too many test failures using this method... is it because of callbacks?

          synchronized (solrCoreState.getUpdateLock()) {
            ulog.preCommit(cmd);
          }

          // todo: refactor this shared code (or figure out why a real CommitUpdateCommand can't be used)
          final Map<String,String> commitData = new HashMap<>();
          commitData.put(SolrIndexWriter.COMMIT_TIME_MSEC_KEY, String.valueOf(System.currentTimeMillis()));
          writer.setCommitData(commitData);
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

      // we went through the normal process to commit, so we don't have to artificially
      // cap any ulog files.
      try {
        if (ulog != null) ulog.close(false);
      }  catch (Throwable th) {
        log.error("Error closing log files", th);
        if (th instanceof OutOfMemoryError) {
          throw (OutOfMemoryError) th;
        }
      }

      if (writer != null) {
        try {
          if (indexWriterCloseWaitsForMerges) writer.waitForMerges();
        } finally {
          writer.close();
        }
      }

    } finally {
      solrCoreState.getCommitLock().unlock();
      if (clearRequestInfo) SolrRequestInfo.clearRequestInfo();
    }
  }

  @Override
  public void split(SplitIndexCommand cmd) throws IOException {
    commit(new CommitUpdateCommand(cmd.req, false));
    SolrIndexSplitter splitter = new SolrIndexSplitter(cmd);
    splitter.split();
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  @Override
  public String getName() {
    return DirectUpdateHandler2.class.getName();
  }

  @Override
  public String getVersion() {
    return SolrCore.version;
  }

  @Override
  public String getDescription() {
    return "Update handler that efficiently directly updates the on-disk main lucene index";
  }

  @Override
  public Category getCategory() {
    return Category.UPDATEHANDLER;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    lst.add("commits", commitCommands.get());
    if (commitTracker.getDocsUpperBound() > 0) {
      lst.add("autocommit maxDocs", commitTracker.getDocsUpperBound());
    }
    if (commitTracker.getTimeUpperBound() > 0) {
      lst.add("autocommit maxTime", "" + commitTracker.getTimeUpperBound() + "ms");
    }
    lst.add("autocommits", commitTracker.getCommitCount());
    if (softCommitTracker.getDocsUpperBound() > 0) {
      lst.add("soft autocommit maxDocs", softCommitTracker.getDocsUpperBound());
    }
    if (softCommitTracker.getTimeUpperBound() > 0) {
      lst.add("soft autocommit maxTime", "" + softCommitTracker.getTimeUpperBound() + "ms");
    }
    lst.add("soft autocommits", softCommitTracker.getCommitCount());
    lst.add("optimizes", optimizeCommands.get());
    lst.add("rollbacks", rollbackCommands.get());
    lst.add("expungeDeletes", expungeDeleteCommands.get());
    lst.add("docsPending", numDocsPending.get());
    // pset.size() not synchronized, but it should be fine to access.
    // lst.add("deletesPending", pset.size());
    lst.add("adds", addCommands.get());
    lst.add("deletesById", deleteByIdCommands.get());
    lst.add("deletesByQuery", deleteByQueryCommands.get());
    lst.add("errors", numErrors.get());
    lst.add("cumulative_adds", addCommandsCumulative.get());
    lst.add("cumulative_deletesById", deleteByIdCommandsCumulative.get());
    lst.add("cumulative_deletesByQuery", deleteByQueryCommandsCumulative.get());
    lst.add("cumulative_errors", numErrorsCumulative.get());
    if (this.ulog != null) {
      lst.add("transaction_logs_total_size", ulog.getTotalLogsSize());
      lst.add("transaction_logs_total_number", ulog.getTotalLogsNumber());
    }
    return lst;
  }

  @Override
  public String toString() {
    return "DirectUpdateHandler2" + getStatistics();
  }
  
  @Override
  public SolrCoreState getSolrCoreState() {
    return solrCoreState;
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
