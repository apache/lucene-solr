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

package org.apache.solr.update;


import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.HitCollector;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import java.util.logging.Logger;
import java.util.Vector;
import java.io.IOException;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.core.*;

import javax.xml.xpath.XPathConstants;

/**
 * <code>UpdateHandler</code> handles requests to change the index
 * (adds, deletes, commits, optimizes, etc).
 *
 * @author yonik
 * @version $Id$
 * @since solr 0.9
 */

public abstract class UpdateHandler implements SolrInfoMBean {
  protected final static Logger log = Logger.getLogger(UpdateHandler.class.getName());

  protected final SolrCore core;
  protected final IndexSchema schema;

  protected final SchemaField idField;
  protected final FieldType idFieldType;

  protected Vector<SolrEventListener> commitCallbacks = new Vector<SolrEventListener>();
  protected Vector<SolrEventListener> optimizeCallbacks = new Vector<SolrEventListener>();

  private void parseEventListeners() {
    NodeList nodes = (NodeList) SolrConfig.config.evaluate("updateHandler/listener[@event=\"postCommit\"]", XPathConstants.NODESET);
    if (nodes!=null) {
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
        try {
          String className = DOMUtil.getAttr(node,"class");
          SolrEventListener listener = (SolrEventListener)Config.newInstance(className);
          listener.init(DOMUtil.childNodesToNamedList(node));
          // listener.init(DOMUtil.toMapExcept(node.getAttributes(),"class","synchronized"));
          commitCallbacks.add(listener);
          log.info("added SolrEventListener for postCommit: " + listener);
        } catch (Exception e) {
          throw new SolrException(1,"error parsing event listevers", e, false);
        }
      }
    }
    nodes = (NodeList)SolrConfig.config.evaluate("updateHandler/listener[@event=\"postOptimize\"]", XPathConstants.NODESET);
    if (nodes!=null) {
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
        try {
          String className = DOMUtil.getAttr(node,"class");
          SolrEventListener listener = (SolrEventListener)Config.newInstance(className);
          listener.init(DOMUtil.childNodesToNamedList(node));
          optimizeCallbacks.add(listener);
          log.info("added SolarEventListener for postOptimize: " + listener);
        } catch (Exception e) {
          throw new SolrException(1,"error parsing event listeners", e, false);
        }
      }
    }
  }

  protected void callPostCommitCallbacks() {
    for (SolrEventListener listener : commitCallbacks) {
      listener.postCommit();
    }
  }

  protected void callPostOptimizeCallbacks() {
    for (SolrEventListener listener : optimizeCallbacks) {
      listener.postCommit();
    }
  }

  public UpdateHandler(SolrCore core)  {
    this.core=core;
    schema = core.getSchema();
    idField = schema.getUniqueKeyField();
    idFieldType = idField!=null ? idField.getType() : null;

    parseEventListeners();
    SolrInfoRegistry.getRegistry().put("updateHandler", this);
  }

  protected SolrIndexWriter createMainIndexWriter(String name) throws IOException {
    SolrIndexWriter writer = new SolrIndexWriter(name,core.getIndexDir(), false, schema,SolrCore.mainIndexConfig);
    return writer;
  }

  protected final Term idTerm(String id) {
    // to correctly create the Term, the string needs to be run
    // through the Analyzer for that field.
    return new Term(idField.getName(), idFieldType.toInternal(id));
  }

  protected final String getId(Document doc) {
    if (idField == null) throw new SolrException(400,"Operation requires schema to have a unique key field");
    String id = doc.get(idField.getName());
    if (id == null) throw new SolrException(400,"Document is missing uniqueKey field " + idField.getName());
    return id;
  }

  protected final String getOptId(Document doc) {
    if (idField == null) return null;
    return doc.get(idField.getName());
  }


  public abstract int addDoc(AddUpdateCommand cmd) throws IOException;
  public abstract void delete(DeleteUpdateCommand cmd) throws IOException;
  public abstract void deleteByQuery(DeleteUpdateCommand cmd) throws IOException;
  public abstract void commit(CommitUpdateCommand cmd) throws IOException;
  public abstract void close() throws IOException;


  class DeleteHitCollector extends HitCollector {
    public int deleted=0;
    public final SolrIndexSearcher searcher;

    public DeleteHitCollector(SolrIndexSearcher searcher) {
      this.searcher = searcher;
    }

    public void collect(int doc, float score) {
      try {
        searcher.getReader().deleteDocument(doc);
        deleted++;
      } catch (IOException e) {
        // don't try to close the searcher on failure for now...
        // try { closeSearcher(); } catch (Exception ee) { SolrException.log(log,ee); }
        throw new SolrException(500,"Error deleting doc# "+doc,e,false);
      }
    }
  }

}


