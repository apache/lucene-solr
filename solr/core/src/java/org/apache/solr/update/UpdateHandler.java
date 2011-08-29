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

package org.apache.solr.update;


import java.io.IOException;
import java.util.Vector;

import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>UpdateHandler</code> handles requests to change the index
 * (adds, deletes, commits, optimizes, etc).
 *
 *
 * @since solr 0.9
 */

public abstract class UpdateHandler implements SolrInfoMBean {
  protected final static Logger log = LoggerFactory.getLogger(UpdateHandler.class);

  protected final SolrCore core;
  protected final IndexSchema schema;

  protected final SchemaField idField;
  protected final FieldType idFieldType;

  protected Vector<SolrEventListener> commitCallbacks = new Vector<SolrEventListener>();
  protected Vector<SolrEventListener> softCommitCallbacks = new Vector<SolrEventListener>();
  protected Vector<SolrEventListener> optimizeCallbacks = new Vector<SolrEventListener>();

  /**
   * Called when a SolrCore using this UpdateHandler is closed.
   */
  public abstract void decref();
  
  /**
   * Called when this UpdateHandler is shared with another SolrCore.
   */
  public abstract void incref();

  private void parseEventListeners() {
    final Class<SolrEventListener> clazz = SolrEventListener.class;
    final String label = "Event Listener";
    for (PluginInfo info : core.getSolrConfig().getPluginInfos(SolrEventListener.class.getName())) {
      String event = info.attributes.get("event");
      if ("postCommit".equals(event)) {
        SolrEventListener obj = core.createInitInstance(info,clazz,label,null);
        commitCallbacks.add(obj);
        log.info("added SolrEventListener for postCommit: " + obj);
      } else if ("postOptimize".equals(event)) {
        SolrEventListener obj = core.createInitInstance(info,clazz,label,null);
        optimizeCallbacks.add(obj);
        log.info("added SolrEventListener for postOptimize: " + obj);
      }
    }
  }

  protected void callPostCommitCallbacks() {
    for (SolrEventListener listener : commitCallbacks) {
      listener.postCommit();
    }
  }

  protected void callPostSoftCommitCallbacks() {
    for (SolrEventListener listener : softCommitCallbacks) {
      listener.postSoftCommit();
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
  }
  
  /**
   * Allows the UpdateHandler to create the SolrIndexSearcher after it
   * has issued a 'softCommit'. 
   * 
   * @param previousSearcher
   * @throws IOException
   */
  public abstract SolrIndexSearcher reopenSearcher(SolrIndexSearcher previousSearcher) throws IOException;
  
  /**
   * Called when the Writer should be opened again - eg when replication replaces
   * all of the index files.
   * 
   * @throws IOException
   */
  public abstract void newIndexWriter() throws IOException;

  public abstract SolrCoreState getIndexWriterProvider();

  public abstract int addDoc(AddUpdateCommand cmd) throws IOException;
  public abstract void delete(DeleteUpdateCommand cmd) throws IOException;
  public abstract void deleteByQuery(DeleteUpdateCommand cmd) throws IOException;
  public abstract int mergeIndexes(MergeIndexesCommand cmd) throws IOException;
  public abstract void commit(CommitUpdateCommand cmd) throws IOException;
  public abstract void rollback(RollbackUpdateCommand cmd) throws IOException;
  public abstract void close() throws IOException;


  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerCommitCallback( SolrEventListener listener )
  {
    commitCallbacks.add( listener );
  }
  
  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerSoftCommitCallback( SolrEventListener listener )
  {
    softCommitCallbacks.add( listener );
  }

  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerOptimizeCallback( SolrEventListener listener )
  {
    optimizeCallbacks.add( listener );
  }
}
