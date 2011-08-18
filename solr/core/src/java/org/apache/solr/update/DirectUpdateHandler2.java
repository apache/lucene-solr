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

/**
 */

package org.apache.solr.update;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrConfig.UpdateHandlerInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;

/**
 *  TODO: add soft commitWithin support
 * 
 * <code>DirectUpdateHandler2</code> implements an UpdateHandler where documents are added
 * directly to the main Lucene index as opposed to adding to a separate smaller index.
 */
public class DirectUpdateHandler2 extends UpdateHandler {
  protected SolrCoreState indexWriterProvider;

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

  public DirectUpdateHandler2(SolrCore core) throws IOException {
    super(core);
   
    indexWriterProvider = new DefaultSolrCoreState(core.getDirectoryFactory());
    
    UpdateHandlerInfo updateHandlerInfo = core.getSolrConfig()
        .getUpdateHandlerInfo();
    int docsUpperBound = updateHandlerInfo.autoCommmitMaxDocs; // getInt("updateHandler/autoCommit/maxDocs", -1);
    int timeUpperBound = updateHandlerInfo.autoCommmitMaxTime; // getInt("updateHandler/autoCommit/maxTime", -1);
    commitTracker = new CommitTracker("Hard", core, docsUpperBound, timeUpperBound, true, false);
    
    int softCommitDocsUpperBound = updateHandlerInfo.autoSoftCommmitMaxDocs; // getInt("updateHandler/autoSoftCommit/maxDocs", -1);
    int softCommitTimeUpperBound = updateHandlerInfo.autoSoftCommmitMaxTime; // getInt("updateHandler/autoSoftCommit/maxTime", -1);
    softCommitTracker = new CommitTracker("Soft", core, softCommitDocsUpperBound, softCommitTimeUpperBound, true, true);
  }
  
  public DirectUpdateHandler2(SolrCore core, UpdateHandler updateHandler) throws IOException {
    super(core);
    if (updateHandler instanceof DirectUpdateHandler2) {
      this.indexWriterProvider = ((DirectUpdateHandler2)updateHandler).indexWriterProvider;
    } else {
      // the impl has changed, so we cannot use the old state - decref it
      updateHandler.decref();
      indexWriterProvider = new DefaultSolrCoreState(core.getDirectoryFactory());
    }
    
    UpdateHandlerInfo updateHandlerInfo = core.getSolrConfig()
        .getUpdateHandlerInfo();
    int docsUpperBound = updateHandlerInfo.autoCommmitMaxDocs; // getInt("updateHandler/autoCommit/maxDocs", -1);
    int timeUpperBound = updateHandlerInfo.autoCommmitMaxTime; // getInt("updateHandler/autoCommit/maxTime", -1);
    commitTracker = new CommitTracker("Hard", core, docsUpperBound, timeUpperBound, true, false);
    
    int softCommitDocsUpperBound = updateHandlerInfo.autoSoftCommmitMaxDocs; // getInt("updateHandler/autoSoftCommit/maxDocs", -1);
    int softCommitTimeUpperBound = updateHandlerInfo.autoSoftCommmitMaxTime; // getInt("updateHandler/autoSoftCommit/maxTime", -1);
    softCommitTracker = new CommitTracker("Soft", core, softCommitDocsUpperBound, softCommitTimeUpperBound, true, true);
    
  }

  private void deleteAll() throws IOException {
    SolrCore.log.info(core.getLogId()+"REMOVING ALL DOCUMENTS FROM INDEX");
    indexWriterProvider.getIndexWriter(core).deleteAll();
  }

  protected void rollbackWriter() throws IOException {
    numDocsPending.set(0);
    indexWriterProvider.rollbackIndexWriter(core);
    
  }

  @Override
  public int addDoc(AddUpdateCommand cmd) throws IOException {
    IndexWriter writer = indexWriterProvider.getIndexWriter(core);
    addCommands.incrementAndGet();
    addCommandsCumulative.incrementAndGet();
    int rc=-1;

    // if there is no ID field, don't overwrite
    if( idField == null ) {
      cmd.overwrite = false;
    }


    try {
      commitTracker.addedDocument( cmd.commitWithin );
      softCommitTracker.addedDocument( cmd.commitWithin );

      if (cmd.overwrite) {
        Term updateTerm;
        Term idTerm = new Term(idField.getName(), cmd.getIndexedId());
        boolean del = false;
        if (cmd.updateTerm == null) {
          updateTerm = idTerm;
        } else {
          del = true;
          updateTerm = cmd.updateTerm;
        }

        writer.updateDocument(updateTerm, cmd.getLuceneDocument());
        if(del) { // ensure id remains unique
          BooleanQuery bq = new BooleanQuery();
          bq.add(new BooleanClause(new TermQuery(updateTerm), Occur.MUST_NOT));
          bq.add(new BooleanClause(new TermQuery(idTerm), Occur.MUST));
          writer.deleteDocuments(bq);
        }
      } else {
        // allow duplicates
        writer.addDocument(cmd.getLuceneDocument());
      }

      rc = 1;
    } finally {
      if (rc!=1) {
        numErrors.incrementAndGet();
        numErrorsCumulative.incrementAndGet();
      } else {
        numDocsPending.incrementAndGet();
      }
    }

    return rc;
  }


  // we don't return the number of docs deleted because it's not always possible to quickly know that info.
  @Override
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    deleteByIdCommands.incrementAndGet();
    deleteByIdCommandsCumulative.incrementAndGet();

    indexWriterProvider.getIndexWriter(core).deleteDocuments(new Term(idField.getName(), cmd.getIndexedId()));

    if (commitTracker.timeUpperBound > 0) {
      commitTracker.scheduleCommitWithin(commitTracker.timeUpperBound);
    } else if (softCommitTracker.timeUpperBound > 0) {
      softCommitTracker.scheduleCommitWithin(softCommitTracker.timeUpperBound);
    }
  }

  // we don't return the number of docs deleted because it's not always possible to quickly know that info.
  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    deleteByQueryCommands.incrementAndGet();
    deleteByQueryCommandsCumulative.incrementAndGet();
    boolean madeIt=false;
    try {
      Query q;
      try {
        QParser parser = QParser.getParser(cmd.query, "lucene", cmd.req);
        q = parser.getQuery();
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      
      boolean delAll = MatchAllDocsQuery.class == q.getClass();
      
      if (delAll) {
        deleteAll();
      } else {
        indexWriterProvider.getIndexWriter(core).deleteDocuments(q);
      }
      
      madeIt = true;
      
      if (commitTracker.timeUpperBound > 0) {
        commitTracker.scheduleCommitWithin(commitTracker.timeUpperBound);
      } else if (softCommitTracker.timeUpperBound > 0) {
        softCommitTracker.scheduleCommitWithin(softCommitTracker.timeUpperBound);
      }
      
    } finally {
      if (!madeIt) {
        numErrors.incrementAndGet();
        numErrorsCumulative.incrementAndGet();
      }
    }
  }

  @Override
  public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
    mergeIndexesCommands.incrementAndGet();
    int rc;

    log.info("start " + cmd);
    
    IndexReader[] readers = cmd.readers;
    if (readers != null && readers.length > 0) {
      indexWriterProvider.getIndexWriter(core).addIndexes(readers);
      rc = 1;
    } else {
      rc = 0;
    }
    log.info("end_mergeIndexes");

    // TODO: consider soft commit issues
    if (rc == 1 && commitTracker.timeUpperBound > 0) {
      commitTracker.scheduleCommitWithin(commitTracker.timeUpperBound);
    } else if (rc == 1 && softCommitTracker.timeUpperBound > 0) {
      softCommitTracker.scheduleCommitWithin(softCommitTracker.timeUpperBound);
    }

    return rc;
  }

  @Override
  public void commit(CommitUpdateCommand cmd) throws IOException {
    IndexWriter writer = indexWriterProvider.getIndexWriter(core);
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
      log.info("start "+cmd);

      if (cmd.optimize) {
        writer.optimize(cmd.maxOptimizeSegments);
      } else if (cmd.expungeDeletes) {
        writer.expungeDeletes();
      }

      if (!cmd.softCommit) {
        writer.commit();
        numDocsPending.set(0);
        callPostCommitCallbacks();
      } else {
        callPostSoftCommitCallbacks();
      }


      if (cmd.optimize) {
        callPostOptimizeCallbacks();
      }


      synchronized (this) {
        if (cmd.softCommit) {
          core.getSearcher(true,false,waitSearcher, true);
        } else {
          core.getSearcher(true,false,waitSearcher);
        }
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
      addCommands.set(0);
      deleteByIdCommands.set(0);
      deleteByQueryCommands.set(0);
      numErrors.set(error ? 1 : 0);
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
  public SolrIndexSearcher reopenSearcher(SolrIndexSearcher previousSearcher) throws IOException {
    
    IndexReader currentReader = previousSearcher.getIndexReader();
    IndexReader newReader;

    newReader = currentReader.reopen(indexWriterProvider.getIndexWriter(core), true);
  
    
    if (newReader == currentReader) {
      currentReader.incRef();
    }

    return new SolrIndexSearcher(core, schema, "main", newReader, true, true, true, core.getDirectoryFactory());
  }
  
  @Override
  public void newIndexWriter() throws IOException {
    indexWriterProvider.newIndexWriter(core);
  }
  
  /**
   * @since Solr 1.4
   */
  @Override
  public void rollback(RollbackUpdateCommand cmd) throws IOException {
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
      numErrors.set(error ? 1 : 0);
    }
  }


  @Override
  public void close() throws IOException {
    log.info("closing " + this);
    
    commitTracker.close();
    softCommitTracker.close();

    numDocsPending.set(0);

    indexWriterProvider.decref();
    
    log.info("closed " + this);
  }


  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  public String getName() {
    return DirectUpdateHandler2.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "Update handler that efficiently directly updates the on-disk main lucene index";
  }

  public Category getCategory() {
    return Category.UPDATEHANDLER;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    lst.add("commits", commitCommands.get());
    if (commitTracker.docsUpperBound > 0) {
      lst.add("autocommit maxDocs", commitTracker.docsUpperBound);
    }
    if (commitTracker.timeUpperBound > 0) {
      lst.add("autocommit maxTime", "" + commitTracker.timeUpperBound + "ms");
    }
    lst.add("autocommits", commitTracker.autoCommitCount);
    if (softCommitTracker.docsUpperBound > 0) {
      lst.add("soft autocommit maxDocs", softCommitTracker.docsUpperBound);
    }
    if (softCommitTracker.timeUpperBound > 0) {
      lst.add("soft autocommit maxTime", "" + softCommitTracker.timeUpperBound + "ms");
    }
    lst.add("soft autocommits", softCommitTracker.autoCommitCount);
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
    return lst;
  }

  @Override
  public String toString() {
    return "DirectUpdateHandler2" + getStatistics();
  }
  
  public SolrCoreState getIndexWriterProvider() {
    return indexWriterProvider;
  }

  @Override
  public void decref() {
    try {
      indexWriterProvider.decref();
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "", e, false);
    }
  }

  @Override
  public void incref() {
    indexWriterProvider.incref();
  }
}
