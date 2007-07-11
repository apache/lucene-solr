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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
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
import java.util.logging.Level;
import java.io.IOException;
import java.net.URL;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrConfig;

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
  AtomicLong commitCommands= new AtomicLong();
  AtomicLong optimizeCommands= new AtomicLong();
  AtomicLong numDocsDeleted= new AtomicLong();
  AtomicLong numDocsPending= new AtomicLong();
  AtomicLong numErrors = new AtomicLong();
  AtomicLong numErrorsCumulative = new AtomicLong();

  // tracks when auto-commit should occur
  protected final CommitTracker tracker;

  // The key is the id, the value (Integer) is the number
  // of docs to save (delete all except the last "n" added)
  protected final Map<String,Integer> pset;

  // commonly used constants for the count in the pset
  protected final static Integer ZERO = 0;
  protected final static Integer ONE = 1;

  // iwCommit protects internal data and open/close of the IndexWriter and
  // is a mutex. Any use of the index writer should be protected by iwAccess, 
  // which admits multiple simultaneous acquisitions.  iwAccess is 
  // mutually-exclusive with the iwCommit lock.
  protected final Lock iwAccess, iwCommit;

  protected IndexWriter writer;
  protected SolrIndexSearcher searcher;

  public DirectUpdateHandler2(SolrCore core) throws IOException {
    super(core);
    /* A TreeMap is used to maintain the natural ordering of the document ids,
       which makes commits more efficient
     */
    pset = new TreeMap<String,Integer>(); 

    ReadWriteLock rwl = new ReentrantReadWriteLock();
    iwAccess = rwl.readLock();
    iwCommit = rwl.writeLock();

    tracker = new CommitTracker();
  }

  // must only be called when iwCommit lock held
  protected void openWriter() throws IOException {
    if (writer==null) {
      writer = createMainIndexWriter("DirectUpdateHandler2");
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

  protected void openSearcher() throws IOException {
    if (searcher==null) {
      searcher = core.newSearcher("DirectUpdateHandler2");
    }
  }

  protected void closeSearcher() throws IOException {
    try {
      if (searcher!=null) searcher.close();
    } finally {
      // if an exception causes a lock to not be
      // released, we could try to delete it.
      searcher=null;
    }
  }

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
        if (!cmd.allowDups && !cmd.overwritePending && !cmd.overwriteCommitted) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unsupported param combo:" + cmd);
          // this would need a reader to implement (to be able to check committed
          // before adding.)
          // return addNoOverwriteNoDups(cmd);
        } else if (!cmd.allowDups && !cmd.overwritePending && cmd.overwriteCommitted) {
          rc = addConditionally(cmd);
      } else if (!cmd.allowDups && cmd.overwritePending && !cmd.overwriteCommitted) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unsupported param combo:" + cmd);
        } else if (!cmd.allowDups && cmd.overwritePending && cmd.overwriteCommitted) {
          rc = overwriteBoth(cmd);
        } else if (cmd.allowDups && !cmd.overwritePending && !cmd.overwriteCommitted) {
          rc = allowDups(cmd);
        } else if (cmd.allowDups && !cmd.overwritePending && cmd.overwriteCommitted) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unsupported param combo:" + cmd);
        } else if (cmd.allowDups && cmd.overwritePending && !cmd.overwriteCommitted) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unsupported param combo:" + cmd);
        } else if (cmd.allowDups && cmd.overwritePending && cmd.overwriteCommitted) {
          rc = overwriteBoth(cmd);
        }
        if (rc == -1)
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"unsupported param combo:" + cmd);
        
        if (rc == 1) {
          // adding document -- prep writer
          closeSearcher();
          openWriter();
          tracker.addedDocument();          
        } else {
          // exit prematurely
          return rc;
        }
      } // end synchronized block

      // this is the only unsynchronized code in the iwAccess block, which
      // should account for most of the time
      assert(rc == 1);
      writer.addDocument(cmd.doc);
      
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
      pset.put(idFieldType.toInternal(cmd.id), ZERO);
    } finally { 
      iwCommit.unlock(); 
    }
    
    if( tracker.timeUpperBound > 0 ) {
      tracker.scheduleCommitWithin( tracker.timeUpperBound );
    }
  }

  // why not return number of docs deleted?
  // Depending on implementation, we may not be able to immediately determine the num...
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
    try {
     Query q = QueryParsing.parseQuery(cmd.query, schema);

     int totDeleted = 0;
     iwCommit.lock();
     try {
       // we need to do much of the commit logic (mainly doing queued
       // deletes since deleteByQuery can throw off our counts.
       doDeletions();
       
       closeWriter();
       openSearcher();

       // if we want to count the number of docs that were deleted, then
       // we need a new instance of the DeleteHitCollector
       final DeleteHitCollector deleter = new DeleteHitCollector(searcher);
       searcher.search(q, null, deleter);
       totDeleted = deleter.deleted;
     } finally {
       iwCommit.unlock();
     }

     if (SolrCore.log.isLoggable(Level.FINE)) {
       SolrCore.log.fine("docs deleted by query:" + totDeleted);
     }
     numDocsDeleted.getAndAdd(totDeleted);
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


  ///////////////////////////////////////////////////////////////////
  /////////////////// helper method for each add type ///////////////
  ///////////////////////////////////////////////////////////////////

  // methods return 1 if the document is to be added; 0 otherwise.
  // methods must be called in synchronized context

  protected int addConditionally(AddUpdateCommand cmd) throws IOException {
    if (cmd.indexedId ==null) {
      cmd.indexedId =getIndexedId(cmd.doc);
    }
    Integer saveCount = pset.get(cmd.indexedId);
    if (saveCount!=null && saveCount!=0) {
      // a doc with this id already exists in the pending set
      return 0;
    }
    pset.put(cmd.indexedId, ONE);
    return 1;
  }


  // overwrite both pending and committed
  protected int overwriteBoth(AddUpdateCommand cmd) throws IOException {
    if (cmd.indexedId ==null) {
      cmd.indexedId =getIndexedId(cmd.doc);
    }
    pset.put(cmd.indexedId, ONE);
    return 1;
  }


  // add without checking
  protected int allowDups(AddUpdateCommand cmd) throws IOException {
    if (cmd.indexedId ==null) {
      cmd.indexedId =getIndexedIdOptional(cmd.doc);
    }
    if (cmd.indexedId != null) {
      Integer saveCount = pset.get(cmd.indexedId);
      
      // if there weren't any docs marked for deletion before, then don't mark
      // any for deletion now.
      if (saveCount == null) return 1;

      // If there were docs marked for deletion, then increment the number of
      // docs to save at the end.

      // the following line is optional, but it saves an allocation in the common case.
      if (saveCount == ZERO) saveCount=ONE;
      else saveCount++;

      pset.put(cmd.indexedId, saveCount);
    }
    return 1;
  }

  //
  // do all needed deletions.
  // call with iwCommit lock held
  //
  protected void doDeletions() throws IOException {
    int[] docnums = new int[0];

    if (pset.size() > 0) { // optimization: only open searcher if there is something to delete...
      log.info("DirectUpdateHandler2 deleting and removing dups for " + pset.size() +" ids");
      int numDeletes=0;

      closeWriter();
      openSearcher();
      IndexReader reader = searcher.getReader();
      TermDocs tdocs = reader.termDocs();
      String fieldname = idField.getName();

      for (Map.Entry<String,Integer> entry : pset.entrySet()) {
        String id = entry.getKey();
        int saveLast = entry.getValue();  // save the last "saveLast" documents

        //expand our array that keeps track of docs if needed.
        if (docnums==null || saveLast > docnums.length) {
          docnums = new int[saveLast];
        }

        // initialize all docnums in the list to -1 (unused)
        for (int i=0; i<saveLast; i++) {
          docnums[i] = -1;
        }

        tdocs.seek(new Term(fieldname,id));

        //
        // record the docs for this term in the "docnums" array and wrap around
        // at size "saveLast".  If we reuse a slot in the array, then we delete
        // the doc that was there from the index.
        //
        int pos=0;
        while (tdocs.next()) {
          if (saveLast==0) {
            // special case - delete all the docs as we see them.
            reader.deleteDocument(tdocs.doc());
            numDeletes++;
            continue;
          }

          int prev=docnums[pos];
          docnums[pos]=tdocs.doc();
          if (prev != -1) {
            reader.deleteDocument(prev);
            numDeletes++;
          }

          if (++pos >= saveLast) pos=0;
        }
      }

      // should we ever shrink it again, or just clear it?
      pset.clear();
      log.info("DirectUpdateHandler2 docs deleted=" + numDeletes);
      numDocsDeleted.addAndGet(numDeletes);
    }

  }



  public void commit(CommitUpdateCommand cmd) throws IOException {

    if (cmd.optimize) {
      optimizeCommands.incrementAndGet();
    } else {
      commitCommands.incrementAndGet();
    }

    Future[] waitSearcher = null;
    if (cmd.waitSearcher) {
      waitSearcher = new Future[1];
    }

    boolean error=true;
    iwCommit.lock();
    try {
      log.info("start "+cmd);
      doDeletions();
        
      if (cmd.optimize) {
        closeSearcher();
        openWriter(); 
        writer.optimize();
      }

      closeSearcher();
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

    return;
  }


  public void close() throws IOException {
    log.info("closing " + this);
    iwCommit.lock();
    try{
      // cancel any pending operations
      if( tracker.pending != null ) {
        tracker.pending.cancel( true );
        tracker.pending = null;
      }
      doDeletions();
      closeSearcher();
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

      docsUpperBound = SolrConfig.config.getInt("updateHandler/autoCommit/maxDocs", -1);
      timeUpperBound = SolrConfig.config.getInt("updateHandler/autoCommit/maxTime", -1);

      SolrCore.log.info("AutoCommit: " + this);
    }

    /** schedeule individual commits */
    public synchronized void scheduleCommitWithin(long commitMaxTime) 
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
    public void addedDocument() {
      docsSinceCommit++;
      lastAddedTime = System.currentTimeMillis();
      // maxDocs-triggered autoCommit
      if( docsUpperBound > 0 && (docsSinceCommit > docsUpperBound) ) {
        if (pending != null && 
            pending.getDelay(TimeUnit.MILLISECONDS) > DOC_COMMIT_DELAY_MS) {
          // another commit is pending, but too far away (probably due to
          // maxTime)
          pending.cancel(false);
          pending = null;
        }
        if (pending == null) {
          // 1/4 second seems fast enough for anyone using maxDocs
          pending = scheduler.schedule(this, DOC_COMMIT_DELAY_MS, 
                                       TimeUnit.MILLISECONDS);
        }
      }
      // maxTime-triggered autoCommit
      if( pending == null && timeUpperBound > 0 ) { 
        // Don't start a new event if one is already waiting 
        pending = scheduler.schedule( this, timeUpperBound, TimeUnit.MILLISECONDS );
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

    /** This is the worker part for the ScheduledFuture **/
    public synchronized void run() {
      long started = System.currentTimeMillis();
      try {
        CommitUpdateCommand command = new CommitUpdateCommand( false );
        command.waitFlush = true;
        command.waitSearcher = true; 
        commit( command );
        autoCommitCount++;
      } 
      catch (Exception e) {
        log.severe( "auto commit error..." );
        e.printStackTrace();
      }
      finally {
        pending = null;
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
    lst.add("docsPending", numDocsPending.get());
    // pset.size() not synchronized, but it should be fine to access.
    lst.add("deletesPending", pset.size());
    lst.add("adds", addCommands.get());
    lst.add("deletesById", deleteByIdCommands.get());
    lst.add("deletesByQuery", deleteByQueryCommands.get());
    lst.add("errors", numErrors.get());
    lst.add("cumulative_adds", addCommandsCumulative.get());
    lst.add("cumulative_deletesById", deleteByIdCommandsCumulative.get());
    lst.add("cumulative_deletesByQuery", deleteByQueryCommandsCumulative.get());
    lst.add("cumulative_errors", numErrorsCumulative.get());
    lst.add("docsDeleted", numDocsDeleted.get());
    return lst;
  }

  public String toString() {
    return "DirectUpdateHandler2" + getStatistics();
  }
}
