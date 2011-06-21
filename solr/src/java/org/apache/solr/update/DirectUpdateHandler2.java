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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.net.URL;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;

/**
 * <code>DirectUpdateHandler2</code> implements an UpdateHandler where documents are added
 * directly to the main Lucene index as opposed to adding to a separate smaller index.
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
  protected final CommitTracker tracker;

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

    tracker = new CommitTracker();
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

    // if there is no ID field, don't overwrite
    if( idField == null ) {
      cmd.overwrite = false;
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
        tracker.addedDocument( cmd.commitWithin );
      } // end synchronized block

      // this is the only unsynchronized code in the iwAccess block, which
      // should account for most of the time
			Term updateTerm = null;

      if (cmd.overwrite) {
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

    iwCommit.lock();
    try {
      openWriter();
      writer.deleteDocuments(idTerm.createTerm(idFieldType.toInternal(cmd.id)));
    } finally {
      iwCommit.unlock();
    }

    if( tracker.timeUpperBound > 0 ) {
      tracker.scheduleCommitWithin( tracker.timeUpperBound );
    }
  }

  // why not return number of docs deleted?
  // Depending on implementation, we may not be able to immediately determine the num...
  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    deleteByQueryCommands.incrementAndGet();
    deleteByQueryCommandsCumulative.incrementAndGet();

    boolean madeIt=false;
    boolean delAll=false;
    try {
      Query q = null;
      try {
        QParser parser = QParser.getParser(cmd.query, "lucene", cmd.req);
        q = parser.getQuery();
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }

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

      if( tracker.timeUpperBound > 0 ) {
        tracker.scheduleCommitWithin( tracker.timeUpperBound );
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

    if (rc == 1 && tracker.timeUpperBound > 0) {
      tracker.scheduleCommitWithin(tracker.timeUpperBound);
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
        writer.optimize(cmd.maxOptimizeSegments);
      } else if (cmd.expungeDeletes) {
        openWriter();
        writer.expungeDeletes();
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
      tracker.didCommit();

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
      tracker.didRollback();

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
      // cancel any pending operations
      if( tracker.pending != null ) {
        tracker.pending.cancel( true );
        tracker.pending = null;
      }
      tracker.scheduler.shutdown();
      closeWriter();
    } finally {
      iwCommit.unlock();
    }
    log.info("closed " + this);
  }

  /** Helper class for tracking autoCommit state.
   *
   * Note: This is purely an implementation detail of autoCommit and will
   * definitely change in the future, so the interface should not be
   * relied-upon
   *
   * Note: all access must be synchronized.
   */
  class CommitTracker implements Runnable
  {
    // scheduler delay for maxDoc-triggered autocommits
    public final int DOC_COMMIT_DELAY_MS = 250;

    // settings, not final so we can change them in testing
    int docsUpperBound;
    long timeUpperBound;

    private final ScheduledExecutorService scheduler =
       Executors.newScheduledThreadPool(1);
    private ScheduledFuture pending;

    // state
    long docsSinceCommit;
    int autoCommitCount = 0;
    long lastAddedTime = -1;

    public CommitTracker() {
      docsSinceCommit = 0;
      pending = null;

      docsUpperBound = core.getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs;   //getInt("updateHandler/autoCommit/maxDocs", -1);
      timeUpperBound = core.getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime;    //getInt("updateHandler/autoCommit/maxTime", -1);

      SolrCore.log.info("AutoCommit: " + this);
    }

    /** schedule individual commits */
    public synchronized void scheduleCommitWithin(long commitMaxTime)
    {
      _scheduleCommitWithin( commitMaxTime );
    }

    private void _scheduleCommitWithin(long commitMaxTime)
    {
      // Check if there is a commit already scheduled for longer then this time
      if( pending != null &&
          pending.getDelay(TimeUnit.MILLISECONDS) >= commitMaxTime )
      {
        pending.cancel(false);
        pending = null;
      }

      // schedule a new commit
      if( pending == null ) {
        pending = scheduler.schedule( this, commitMaxTime, TimeUnit.MILLISECONDS );
      }
    }

    /** Indicate that documents have been added
     */
    public void addedDocument( int commitWithin ) {
      docsSinceCommit++;
      lastAddedTime = System.currentTimeMillis();
      // maxDocs-triggered autoCommit
      if( docsUpperBound > 0 && (docsSinceCommit > docsUpperBound) ) {
        _scheduleCommitWithin( DOC_COMMIT_DELAY_MS );
      }

      // maxTime-triggered autoCommit
      long ctime = (commitWithin>0) ? commitWithin : timeUpperBound;
      if( ctime > 0 ) {
        _scheduleCommitWithin( ctime );
      }
    }

    /** Inform tracker that a commit has occurred, cancel any pending commits */
    public void didCommit() {
      if( pending != null ) {
        pending.cancel(false);
        pending = null; // let it start another one
      }
      docsSinceCommit = 0;
    }

    /** Inform tracker that a rollback has occurred, cancel any pending commits */
    public void didRollback() {
      if( pending != null ) {
        pending.cancel(false);
        pending = null; // let it start another one
      }
      docsSinceCommit = 0;
    }

    /** This is the worker part for the ScheduledFuture **/
    public synchronized void run() {
      long started = System.currentTimeMillis();
      SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
      try {
        CommitUpdateCommand command = new CommitUpdateCommand(req, false );
        command.waitFlush = true;
        command.waitSearcher = true;
        //no need for command.maxOptimizeSegments = 1;  since it is not optimizing
        commit( command );
        autoCommitCount++;
      }
      catch (Exception e) {
        log.error( "auto commit error..." );
        e.printStackTrace();
      }
      finally {
        pending = null;
        req.close();
      }

      // check if docs have been submitted since the commit started
      if( lastAddedTime > started ) {
        if( docsUpperBound > 0 && docsSinceCommit > docsUpperBound ) {
          pending = scheduler.schedule( this, 100, TimeUnit.MILLISECONDS );
        }
        else if( timeUpperBound > 0 ) {
          pending = scheduler.schedule( this, timeUpperBound, TimeUnit.MILLISECONDS );
        }
      }
    }

    // to facilitate testing: blocks if called during commit
    public synchronized int getCommitCount() { return autoCommitCount; }

    @Override
    public String toString() {
      if(timeUpperBound > 0 || docsUpperBound > 0) {
        return
          (timeUpperBound > 0 ? ("if uncommited for " + timeUpperBound + "ms; ") : "") +
          (docsUpperBound > 0 ? ("if " + docsUpperBound + " uncommited docs ") : "");

      } else {
        return "disabled";
      }
    }
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
    if (tracker.docsUpperBound > 0) {
      lst.add("autocommit maxDocs", tracker.docsUpperBound);
    }
    if (tracker.timeUpperBound > 0) {
      lst.add("autocommit maxTime", "" + tracker.timeUpperBound + "ms");
    }
    lst.add("autocommits", tracker.autoCommitCount);
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
