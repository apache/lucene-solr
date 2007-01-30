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
 * @author yonik
 */

package org.apache.solr.update;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.util.HashSet;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.io.IOException;
import java.net.URL;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.SimpleOrderedMap;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;

/**
 * <code>DirectUpdateHandler</code> implements an UpdateHandler where documents are added
 * directly to the main lucene index as opposed to adding to a separate smaller index.
 * For this reason, not all combinations to/from pending and committed are supported.
 *
 * @author yonik
 * @version $Id$
 * @since solr 0.9
 */

public class DirectUpdateHandler extends UpdateHandler {

  // the set of ids in the "pending set" (those docs that have been added, but
  // that are not yet visible.
  final HashSet<String> pset;
  IndexWriter writer;
  SolrIndexSearcher searcher;
  int numAdds=0;     // number of docs added to the pending set
  int numPending=0;  // number of docs currently in this pending set
  int numDeleted=0;  // number of docs deleted or


  public DirectUpdateHandler(SolrCore core) throws IOException {
    super(core);
    pset = new HashSet<String>(256);
  }


  protected void openWriter() throws IOException {
    if (writer==null) {
      writer = createMainIndexWriter("DirectUpdateHandler");
    }
  }

  protected void closeWriter() throws IOException {
    try {
      if (writer!=null) writer.close();
    } finally {
      // TODO: if an exception causes the writelock to not be
      // released, we could delete it here.
      writer=null;
    }
  }

  protected void openSearcher() throws IOException {
    if (searcher==null) {
      searcher = core.newSearcher("DirectUpdateHandler");
    }
  }

  protected void closeSearcher() throws IOException {
    try {
      if (searcher!=null) searcher.close();
    } finally {
      // TODO: if an exception causes the writelock to not be
      // released, we could delete it here.
      searcher=null;
    }
  }

  protected void doAdd(Document doc) throws IOException {
    closeSearcher(); openWriter();
    writer.addDocument(doc);
  }

  protected boolean existsInIndex(String indexedId) throws IOException {
    if (idField == null) throw new SolrException(2,"Operation requires schema to have a unique key field");

    closeWriter();
    openSearcher();
    IndexReader ir = searcher.getReader();
    TermDocs tdocs = null;
    boolean exists=false;
    try {
      tdocs = ir.termDocs(idTerm(indexedId));
      if (tdocs.next()) exists=true;
    } finally {
      try { if (tdocs != null) tdocs.close(); } catch (Exception e) {}
    }
    return exists;
  }


  protected int deleteInIndex(String indexedId) throws IOException {
    if (idField == null) throw new SolrException(2,"Operation requires schema to have a unique key field");

    closeWriter(); openSearcher();
    IndexReader ir = searcher.getReader();
    TermDocs tdocs = null;
    int num=0;
    try {
      Term term = new Term(idField.getName(), indexedId);
      num = ir.deleteDocuments(term);
      if (SolrCore.log.isLoggable(Level.FINEST)) {
        SolrCore.log.finest("deleted " + num + " docs matching id " + idFieldType.indexedToReadable(indexedId));
      }
    } finally {
      try { if (tdocs != null) tdocs.close(); } catch (Exception e) {}
    }
    return num;
  }

  protected void overwrite(String indexedId, Document doc) throws IOException {
    if (indexedId ==null) indexedId =getIndexedId(doc);
    deleteInIndex(indexedId);
    doAdd(doc);
  }

  /************** Direct update handler - pseudo code ***********
  def add(doc, id, allowDups, overwritePending, overwriteCommitted):
    if not overwritePending and not overwriteCommitted:
      #special case... no need to check pending set, and we don't keep
      #any state around about this addition
      if allowDups:
        committed[id]=doc  #100
        return
      else:
        #if no dups allowed, we must check the *current* index (pending and committed)
        if not committed[id]: committed[id]=doc  #000
        return
    #001  (searchd addConditionally)
    if not allowDups and not overwritePending and pending[id]: return
    del committed[id]  #delete from pending and committed  111 011
    committed[id]=doc
    pending[id]=True
  ****************************************************************/

  // could return the number of docs deleted, but is that always possible to know???
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    if (!cmd.fromPending && !cmd.fromCommitted)
      throw new SolrException(400,"meaningless command: " + cmd);
    if (!cmd.fromPending || !cmd.fromCommitted)
      throw new SolrException(400,"operation not supported" + cmd);
    String indexedId = idFieldType.toInternal(cmd.id);
    synchronized(this) {
      deleteInIndex(indexedId);
      pset.remove(indexedId);
    }
  }

  // TODO - return number of docs deleted?
  // Depending on implementation, we may not be able to immediately determine num...
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    if (!cmd.fromPending && !cmd.fromCommitted)
      throw new SolrException(400,"meaningless command: " + cmd);
    if (!cmd.fromPending || !cmd.fromCommitted)
      throw new SolrException(400,"operation not supported" + cmd);

    Query q = QueryParsing.parseQuery(cmd.query, schema);

    int totDeleted = 0;
    synchronized(this) {
      closeWriter(); openSearcher();

      // if we want to count the number of docs that were deleted, then
      // we need a new instance of the DeleteHitCollector
      final DeleteHitCollector deleter = new DeleteHitCollector(searcher);
      searcher.search(q, null, deleter);
      totDeleted = deleter.deleted;
    }

    if (SolrCore.log.isLoggable(Level.FINE)) {
      SolrCore.log.fine("docs deleted:" + totDeleted);
    }

  }

  /**************** old hit collector... new one is in base class
  // final DeleteHitCollector deleter = new DeleteHitCollector();
  class DeleteHitCollector extends HitCollector {
    public int deleted=0;
    public void collect(int doc, float score) {
      try {
        searcher.getReader().delete(doc);
        deleted++;
      } catch (IOException e) {
        try { closeSearcher(); } catch (Exception ee) { SolrException.log(SolrCore.log,ee); }
        SolrException.log(SolrCore.log,e);
        throw new SolrException(500,"Error deleting doc# "+doc,e);
      }
    }
  }
  ***************************/

  public void commit(CommitUpdateCommand cmd) throws IOException {
    Future[] waitSearcher = null;
    if (cmd.waitSearcher) {
      waitSearcher = new Future[1];
    }

    synchronized (this) {
      pset.clear();
      closeSearcher();  // flush any deletes
      if (cmd.optimize) {
        openWriter();  // writer needs to be open to optimize
        writer.optimize();
      }
      closeWriter();

      callPostCommitCallbacks();
      if (cmd.optimize) {
        callPostOptimizeCallbacks();
      }

      core.getSearcher(true,false,waitSearcher);
    }

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



  ///////////////////////////////////////////////////////////////////
  /////////////////// helper method for each add type ///////////////
  ///////////////////////////////////////////////////////////////////

  protected int addNoOverwriteNoDups(AddUpdateCommand cmd) throws IOException {
    if (cmd.indexedId ==null) {
      cmd.indexedId =getIndexedId(cmd.doc);
    }
    synchronized (this) {
      if (existsInIndex(cmd.indexedId)) return 0;
      doAdd(cmd.doc);
    }
    return 1;
  }

  protected int addConditionally(AddUpdateCommand cmd) throws IOException {
    if (cmd.indexedId ==null) {
      cmd.indexedId =getIndexedId(cmd.doc);
    }
    synchronized(this) {
      if (pset.contains(cmd.indexedId)) return 0;
      // since case 001 is currently the only case to use pset, only add
      // to it in that instance.
      pset.add(cmd.indexedId);
      overwrite(cmd.indexedId,cmd.doc);
      return 1;
    }
  }


  // overwrite both pending and committed
  protected synchronized int overwriteBoth(AddUpdateCommand cmd) throws IOException {
    overwrite(cmd.indexedId, cmd.doc);
    return 1;
  }


  // add without checking
  protected synchronized int allowDups(AddUpdateCommand cmd) throws IOException {
    doAdd(cmd.doc);
    return 1;
  }


  public int addDoc(AddUpdateCommand cmd) throws IOException {
    if (!cmd.allowDups && !cmd.overwritePending && !cmd.overwriteCommitted) {
      return addNoOverwriteNoDups(cmd);
    } else if (!cmd.allowDups && !cmd.overwritePending && cmd.overwriteCommitted) {
      return addConditionally(cmd);
    } else if (!cmd.allowDups && cmd.overwritePending && !cmd.overwriteCommitted) {
      // return overwriteBoth(cmd);
      throw new SolrException(400,"unsupported param combo:" + cmd);
    } else if (!cmd.allowDups && cmd.overwritePending && cmd.overwriteCommitted) {
      return overwriteBoth(cmd);
    } else if (cmd.allowDups && !cmd.overwritePending && !cmd.overwriteCommitted) {
      return allowDups(cmd);
    } else if (cmd.allowDups && !cmd.overwritePending && cmd.overwriteCommitted) {
      // return overwriteBoth(cmd);
      throw new SolrException(400,"unsupported param combo:" + cmd);
    } else if (cmd.allowDups && cmd.overwritePending && !cmd.overwriteCommitted) {
      // return overwriteBoth(cmd);
      throw new SolrException(400,"unsupported param combo:" + cmd);
    } else if (cmd.allowDups && cmd.overwritePending && cmd.overwriteCommitted) {
      return overwriteBoth(cmd);
    }
    throw new SolrException(400,"unsupported param combo:" + cmd);
  }

  public void close() throws IOException {
    synchronized(this) {
      closeSearcher();
      closeWriter();
    }
  }



  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  public String getName() {
    return DirectUpdateHandler.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "Update handler that directly changes the on-disk main lucene index";
  }

  public Category getCategory() {
    return Category.CORE;
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
    return lst;
  }




}
