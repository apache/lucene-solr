/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * @author yonik
 */

package org.apache.solr.update;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.io.IOException;
import java.net.URL;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;

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
 * @author yonik
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



  // The key is the id, the value (Integer) is the number
  // of docs to save (delete all except the last "n" added)
  protected final HashMap<String,Integer> pset;

  // commonly used constants for the count in the pset
  protected final static Integer ZERO = 0;
  protected final static Integer ONE = 1;

  protected IndexWriter writer;
  protected SolrIndexSearcher searcher;

  public DirectUpdateHandler2(SolrCore core) throws IOException {
    super(core);
    pset = new HashMap<String,Integer>(256); // 256 is just an optional head-start
  }

  protected void openWriter() throws IOException {
    if (writer==null) {
      writer = createMainIndexWriter("DirectUpdateHandler2");
    }
  }

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

  protected void doAdd(Document doc) throws IOException {
    closeSearcher(); openWriter();
    writer.addDocument(doc);
  }



  public int addDoc(AddUpdateCommand cmd) throws IOException {
    addCommands.incrementAndGet();
    addCommandsCumulative.incrementAndGet();
    int rc=-1;
    try {
      if (!cmd.allowDups && !cmd.overwritePending && !cmd.overwriteCommitted) {
        throw new SolrException(400,"unsupported param combo:" + cmd);
        // this would need a reader to implement (to be able to check committed
        // before adding.)
        // return addNoOverwriteNoDups(cmd);
      } else if (!cmd.allowDups && !cmd.overwritePending && cmd.overwriteCommitted) {
        rc = addConditionally(cmd);
        return rc;
      } else if (!cmd.allowDups && cmd.overwritePending && !cmd.overwriteCommitted) {
        throw new SolrException(400,"unsupported param combo:" + cmd);
      } else if (!cmd.allowDups && cmd.overwritePending && cmd.overwriteCommitted) {
        rc = overwriteBoth(cmd);
        return rc;
      } else if (cmd.allowDups && !cmd.overwritePending && !cmd.overwriteCommitted) {
        rc = allowDups(cmd);
        return rc;
      } else if (cmd.allowDups && !cmd.overwritePending && cmd.overwriteCommitted) {
        throw new SolrException(400,"unsupported param combo:" + cmd);
      } else if (cmd.allowDups && cmd.overwritePending && !cmd.overwriteCommitted) {
        throw new SolrException(400,"unsupported param combo:" + cmd);
      } else if (cmd.allowDups && cmd.overwritePending && cmd.overwriteCommitted) {
        rc = overwriteBoth(cmd);
        return rc;
      }
      throw new SolrException(400,"unsupported param combo:" + cmd);
    } finally {
      if (rc!=1) {
        numErrors.incrementAndGet();
        numErrorsCumulative.incrementAndGet();
      } else {
        numDocsPending.incrementAndGet();
      }
    }
  }


  // could return the number of docs deleted, but is that always possible to know???
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    deleteByIdCommands.incrementAndGet();
    deleteByIdCommandsCumulative.incrementAndGet();

    if (!cmd.fromPending && !cmd.fromCommitted) {
      numErrors.incrementAndGet();
      numErrorsCumulative.incrementAndGet();
      throw new SolrException(400,"meaningless command: " + cmd);
    }
    if (!cmd.fromPending || !cmd.fromCommitted) {
      numErrors.incrementAndGet();
      numErrorsCumulative.incrementAndGet();
      throw new SolrException(400,"operation not supported" + cmd);
    }

    synchronized(this) {
      pset.put(cmd.id, ZERO);
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
       throw new SolrException(400,"meaningless command: " + cmd);
     }
     if (!cmd.fromPending || !cmd.fromCommitted) {
       numErrors.incrementAndGet();
       numErrorsCumulative.incrementAndGet();
       throw new SolrException(400,"operation not supported" + cmd);
     }

    boolean madeIt=false;
    try {
     Query q = QueryParsing.parseQuery(cmd.query, schema);

     int totDeleted = 0;
     synchronized(this) {
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
     }

     if (SolrCore.log.isLoggable(Level.FINE)) {
       SolrCore.log.fine("docs deleted by query:" + totDeleted);
     }
     numDocsDeleted.getAndAdd(totDeleted);
     madeIt=true;
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


  protected int addConditionally(AddUpdateCommand cmd) throws IOException {
    if (cmd.id==null) {
      cmd.id=getId(cmd.doc);
    }
    synchronized(this) {
      Integer saveCount = pset.get(cmd.id);
      if (saveCount!=null && saveCount!=0) {
        // a doc with this id already exists in the pending set
        return 0;
      }
      pset.put(cmd.id, ONE);
      doAdd(cmd.doc);
      return 1;
    }
  }


  // overwrite both pending and committed
  protected synchronized int overwriteBoth(AddUpdateCommand cmd) throws IOException {
    if (cmd.id==null) {
      cmd.id=getId(cmd.doc);
    }
    synchronized (this) {
      pset.put(cmd.id, ONE);
      doAdd(cmd.doc);
    }
    return 1;
  }


  // add without checking
  protected synchronized int allowDups(AddUpdateCommand cmd) throws IOException {
    if (cmd.id==null) {
      cmd.id=getOptId(cmd.doc);
    }
    synchronized(this) {
      doAdd(cmd.doc);

      if (cmd.id != null) {
        Integer saveCount = pset.get(cmd.id);

        // if there weren't any docs marked for deletion before, then don't mark
        // any for deletion now.
        if (saveCount == null) return 1;

        // If there were docs marked for deletion, then increment the number of
        // docs to save at the end.

        // the following line is optional, but it saves an allocation in the common case.
        if (saveCount == ZERO) saveCount=ONE;
        else saveCount++;

        pset.put(cmd.id, saveCount);
      }
    }
    return 1;
  }

  // NOT FOR USE OUTSIDE OF A "synchronized(this)" BLOCK
  private int[] docnums;

  //
  // do all needed deletions.
  // call in a synchronized context.
  //
  protected void doDeletions() throws IOException {

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
    try {
      synchronized (this) {
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

        log.info("end_commit_flush");
      }  // end synchronized block

      error=false;
    }
    finally {
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
    synchronized(this) {
      doDeletions();
      closeSearcher();
      closeWriter();
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
    NamedList lst = new NamedList();
    lst.add("commits", commitCommands.get());
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
