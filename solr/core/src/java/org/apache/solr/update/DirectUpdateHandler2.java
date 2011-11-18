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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.QueryParsing;

/**
 * <code>DirectUpdateHandler2</code> implements an UpdateHandler where documents are added
 * directly to the main Lucene index as opposed to adding to a separate smaller index.
 * For this reason, not all combinations to/from pending and committed are supported.
 * This version supports efficient removal of duplicates on a commit.  It works by maintaining
 * a related count for every document being added or deleted.  At commit time, for every id with a count,
 * all but the last "count" docs with that id are deleted.
 * <p>
 *
 * Supported add command parameters:
 <TABLE BORDER>
  <TR>
    <TH>allowDups</TH>
    <TH>overwritePending</TH>
    <TH>overwriteCommitted</TH>
    <TH>efficiency</TH>
  </TR>
  <TR>
        <TD>false</TD>
        <TD>false</TD>
        <TD>true</TD>

        <TD>fast</TD>
  </TR>
  <TR>
        <TD>true or false</TD>
        <TD>true</TD>
        <TD>true</TD>

        <TD>fast</TD>
  </TR>
  <TR>
        <TD>true</TD>
        <TD>false</TD>
        <TD>false</TD>
        <TD>fastest</TD>
  </TR>

</TABLE>

 <p>Supported delete commands:
 <TABLE BORDER>
  <TR>
    <TH>command</TH>
    <TH>fromPending</TH>
    <TH>fromCommitted</TH>
    <TH>efficiency</TH>
  </TR>
  <TR>
        <TD>delete</TD>
        <TD>true</TD>
        <TD>true</TD>
        <TD>fast</TD>
  </TR>
  <TR>
        <TD>deleteByQuery</TD>
        <TD>true</TD>
        <TD>true</TD>
        <TD>very slow*</TD>
  </TR>
</TABLE>

  <p>* deleteByQuery causes a commit to happen (close current index writer, open new index reader)
  before it can be processed.  If deleteByQuery functionality is needed, it's best if they can
  be batched and executed together so they may share the same index reader.

 *
 * @version $Id$
 * @since solr 0.9
 */

public class DirectUpdateHandler2 extends UpdateHandler {

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

  // iwCommit protects internal data and open/close of the IndexWriter and
  // is a mutex. Any use of the index writer should be protected by iwAccess, 
  // which admits multiple simultaneous acquisitions.  iwAccess is 
  // mutually-exclusive with the iwCommit lock.
  protected final Lock iwAccess, iwCommit;

  protected IndexWriter writer;

  public DirectUpdateHandler2(SolrCore core) throws IOException {
    super(core);

    // Pass fairness=true so commit request is not starved
    // when add/updates are running hot (SOLR-2342):
    ReadWriteLock rwl = new ReentrantReadWriteLock(true);
    iwAccess = rwl.readLock();
    iwCommit = rwl.writeLock();

    commitTracker = new CommitTracker("commitTracker", core,
        core.getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs,
        core.getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime, true, false);
  }

  // must only be called when iwCommit lock held
  private void deleteAll() throws IOException {
    core.log.info(core.getLogId()+"REMOVING ALL DOCUMENTS FROM INDEX");
    closeWriter();
    writer = createMainIndexWriter("DirectUpdateHandler2", true);
  }

  // must only be called when iwCommit lock held
  protected void openWriter() throws IOException {
    if (writer==null) {
      writer = createMainIndexWriter("DirectUpdateHandler2", false);
    }
  }

  // must only be called when iwCommit lock held
  protected void closeWriter() throws IOException {
    try {
      numDocsPending.set(0);
      if (writer!=null) writer.close();
    } finally {
      // if an exception causes the writelock to not be
      // released, we could try and delete it here
      writer=null;
    }
  }

  // must only be called when iwCommit lock held
  protected void rollbackWriter() throws IOException {
    try {
      numDocsPending.set(0);
      if (writer!=null) writer.rollback();
    } finally {
      writer = null;
    }
  }

  @Override
  public int addDoc(AddUpdateCommand cmd) throws IOException {
    addCommands.incrementAndGet();
    addCommandsCumulative.incrementAndGet();
    int rc=-1;

    // if there is no ID field, use allowDups
    if( idField == null ) {
      cmd.allowDups = true;
      cmd.overwriteCommitted = false;
      cmd.overwritePending = false;
    }

    iwAccess.lock();
    try {

      // We can't use iwCommit to protect internal data here, since it would
      // block other addDoc calls.  Hence, we synchronize to protect internal
      // state.  This is safe as all other state-changing operations are
      // protected with iwCommit (which iwAccess excludes from this block).
      synchronized (this) {
        // adding document -- prep writer
        openWriter();
        commitTracker.addedDocument( cmd.commitWithin );
      } // end synchronized block

      // this is the only unsynchronized code in the iwAccess block, which
      // should account for most of the time
			Term updateTerm = null;

      if (cmd.overwriteCommitted || cmd.overwritePending) {
        if (cmd.indexedId == null) {
          cmd.indexedId = getIndexedId(cmd.doc);
        }
        Term idTerm = this.idTerm.createTerm(cmd.indexedId);
        boolean del = false;
        if (cmd.updateTerm == null) {
          updateTerm = idTerm;
        } else {
          del = true;
        	updateTerm = cmd.updateTerm;
        }

        writer.updateDocument(updateTerm, cmd.getLuceneDocument(schema));
        if(del) { // ensure id remains unique
          BooleanQuery bq = new BooleanQuery();
          bq.add(new BooleanClause(new TermQuery(updateTerm), Occur.MUST_NOT));
          bq.add(new BooleanClause(new TermQuery(idTerm), Occur.MUST));
          writer.deleteDocuments(bq);
        }
      } else {
        // allow duplicates
        writer.addDocument(cmd.getLuceneDocument(schema));
      }

      rc = 1;
    } finally {
      iwAccess.unlock();
      if (rc!=1) {
        numErrors.incrementAndGet();
        numErrorsCumulative.incrementAndGet();
      } else {
        numDocsPending.incrementAndGet();
      }
    }

    return rc;
  }


  // could return the number of docs deleted, but is that always possible to know???
  @Override
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    deleteByIdCommands.incrementAndGet();
    deleteByIdCommandsCumulative.incrementAndGet();

    if (!cmd.fromPending && !cmd.fromCommitted) {
      numErrors.incrementAndGet();
      numErrorsCumulative.incrementAndGet();
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"meaningless command: " + cmd);
    }
    if (!cmd.fromPending || !cmd.fromCommitted) {
      numErrors.incrementAndGet();
      numErrorsCumulative.incrementAndGet();
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"operation not supported" + cmd);
    }

    iwCommit.lock();
    try {
      openWriter();
      writer.deleteDocuments(idTerm.createTerm(idFieldType.toInternal(cmd.id)));
    } finally {
      iwCommit.unlock();
    }

    commitTracker.scheduleCommitWithin(commitTracker.getTimeUpperBound());
  }

  // why not return number of docs deleted?
  // Depending on implementation, we may not be able to immediately determine the num...
   @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
     deleteByQueryCommands.incrementAndGet();
     deleteByQueryCommandsCumulative.incrementAndGet();

     if (!cmd.fromPending && !cmd.fromCommitted) {
       numErrors.incrementAndGet();
       numErrorsCumulative.incrementAndGet();
       throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"meaningless command: " + cmd);
     }
     if (!cmd.fromPending || !cmd.fromCommitted) {
       numErrors.incrementAndGet();
       numErrorsCumulative.incrementAndGet();
       throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"operation not supported" + cmd);
     }

    boolean madeIt=false;
    boolean delAll=false;
    try {
     Query q = QueryParsing.parseQuery(cmd.query, schema);
     delAll = MatchAllDocsQuery.class == q.getClass();

     iwCommit.lock();
     try {
       if (delAll) {
         deleteAll();
       } else {
        openWriter();
        writer.deleteDocuments(q);
       }
     } finally {
       iwCommit.unlock();
     }

     madeIt=true;

     commitTracker.scheduleCommitWithin(commitTracker.getTimeUpperBound());

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
    int rc = -1;

    iwCommit.lock();
    try {
      log.info("start " + cmd);

      IndexReader[] readers = cmd.readers;
      if (readers != null && readers.length > 0) {
        openWriter();
        writer.addIndexes(readers);
        rc = 1;
      } else {
        rc = 0;
      }
      log.info("end_mergeIndexes");
    } finally {
      iwCommit.unlock();
    }

    if (rc == 1 && commitTracker.getTimeUpperBound() > 0) {
      commitTracker.scheduleCommitWithin(commitTracker.getTimeUpperBound());
    }

    return rc;
  }

   public void forceOpenWriter() throws IOException  {
    iwCommit.lock();
    try {
      openWriter();
    } finally {
      iwCommit.unlock();
    }
  }

  @Override
  public void commit(CommitUpdateCommand cmd) throws IOException {

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
    iwCommit.lock();
    try {
      log.info("start "+cmd);

      if (cmd.optimize) {
        openWriter();
        writer.forceMerge(cmd.maxOptimizeSegments);
      } else if (cmd.expungeDeletes) {
        openWriter();
        writer.forceMergeDeletes();
      }
      
      closeWriter();

      callPostCommitCallbacks();
      if (cmd.optimize) {
        callPostOptimizeCallbacks();
      }
      // open a new searcher in the sync block to avoid opening it
      // after a deleteByQuery changed the index, or in between deletes
      // and adds of another commit being done.
      core.getSearcher(true,false,waitSearcher);

      // reset commit tracking
      commitTracker.didCommit();

      log.info("end_commit_flush");

      error=false;
    }
    finally {
      iwCommit.unlock();
      addCommands.set(0);
      deleteByIdCommands.set(0);
      deleteByQueryCommands.set(0);
      numErrors.set(error ? 1 : 0);
    }

    // if we are supposed to wait for the searcher to be registered, then we should do it
    // outside of the synchronized block so that other update operations can proceed.
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

  /**
   * @since Solr 1.4
   */
  @Override
  public void rollback(RollbackUpdateCommand cmd) throws IOException {

    rollbackCommands.incrementAndGet();

    boolean error=true;
    iwCommit.lock();
    try {
      log.info("start "+cmd);

      rollbackWriter();

      //callPostRollbackCallbacks();

      // reset commit tracking
      commitTracker.didRollback();

      log.info("end_rollback");

      error=false;
    }
    finally {
      iwCommit.unlock();
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
    iwCommit.lock();
    try{
      commitTracker.close();
      closeWriter();
    } finally {
      iwCommit.unlock();
    }
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
    if (commitTracker.getTimeUpperBound() > 0) {
      lst.add("autocommit maxDocs", commitTracker.getDocsUpperBound());
    }
    if (commitTracker.getTimeUpperBound() > 0) {
      lst.add("autocommit maxTime", "" + commitTracker.getTimeUpperBound() + "ms");
    }
    lst.add("autocommits", commitTracker.getCommitCount());

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
}
