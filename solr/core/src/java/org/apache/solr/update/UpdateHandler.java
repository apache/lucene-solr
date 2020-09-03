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

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Vector;

/**
 * <code>UpdateHandler</code> handles requests to change the index
 * (adds, deletes, commits, optimizes, etc).
 *
 *
 * @since solr 0.9
 */
public abstract class
UpdateHandler implements SolrInfoBean, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCore core;

  protected final SchemaField idField;
  protected final FieldType idFieldType;

  protected Vector<SolrEventListener> commitCallbacks = new Vector<>();
  protected Vector<SolrEventListener> softCommitCallbacks = new Vector<>();
  protected Vector<SolrEventListener> optimizeCallbacks = new Vector<>();

  protected final UpdateLog ulog;

  protected SolrMetricsContext solrMetricsContext;
  protected volatile boolean closed;

  private void parseEventListeners() {
    final Class<SolrEventListener> clazz = SolrEventListener.class;
    final String label = "Event Listener";
    for (PluginInfo info : core.getSolrConfig().getPluginInfos(SolrEventListener.class.getName())) {
      String event = info.attributes.get("event");
      if ("postCommit".equals(event)) {
        SolrEventListener obj = core.createInitInstance(info,clazz,label,null);
        commitCallbacks.add(obj);
        log.info("added SolrEventListener for postCommit: {}", obj);
      } else if ("postOptimize".equals(event)) {
        SolrEventListener obj = core.createInitInstance(info,clazz,label,null);
        optimizeCallbacks.add(obj);
        log.info("added SolrEventListener for postOptimize: {}", obj);
      }
    }
  }

  /**
   * Call the {@link SolrCoreAware#inform(SolrCore)} on all the applicable registered listeners.
   */
  public void informEventListeners(SolrCore core) {
    for (SolrEventListener listener: commitCallbacks) {
      if (listener instanceof SolrCoreAware) {
        ((SolrCoreAware) listener).inform(core);
      }
    }
    for (SolrEventListener listener: optimizeCallbacks) {
      if (listener instanceof SolrCoreAware) {
        ((SolrCoreAware) listener).inform(core);
      }
    }
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    if (ulog != null) ulog.close();
    ObjectReleaseTracker.release(this);
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
    this(core, null);
  }
  
  public UpdateHandler(SolrCore core, UpdateLog updateLog)  {
    UpdateLog ourUpdateLog = null;
    ObjectReleaseTracker.track(this);
    try {
      this.core = core;
      CoreDescriptor cd = core.getCoreDescriptor();
      if (cd == null) {
        throw new AlreadyClosedException();
      }
      idField = core.getLatestSchema().getUniqueKeyField();
      idFieldType = idField != null ? idField.getType() : null;
      parseEventListeners();
      PluginInfo ulogPluginInfo = core.getSolrConfig().getPluginInfo(UpdateLog.class.getName());

      // If this is a replica of type PULL, don't create the update log
      boolean skipUpdateLog = cd.getCloudDescriptor() != null && !cd.getCloudDescriptor().requiresTransactionLog();
      if (updateLog == null && ulogPluginInfo != null && ulogPluginInfo.isEnabled() && !skipUpdateLog) {
        DirectoryFactory dirFactory = core.getDirectoryFactory();
        if (dirFactory instanceof HdfsDirectoryFactory) {
          ourUpdateLog = new HdfsUpdateLog(((HdfsDirectoryFactory) dirFactory).getConfDir());
        } else {
          String className = ulogPluginInfo.className == null ? UpdateLog.class.getName() : ulogPluginInfo.className;
          ourUpdateLog = core.getResourceLoader().newInstance(className, UpdateLog.class, "update.");
        }

        if (!core.isReloaded() && !dirFactory.isPersistent()) {
          ourUpdateLog.clearLog(core, ulogPluginInfo);
        }

        if (log.isInfoEnabled()) {
          log.info("Using UpdateLog implementation: {}", ourUpdateLog.getClass().getName());
        }
        ourUpdateLog.init(ulogPluginInfo);
        ourUpdateLog.init(this, core);
      } else {
        ourUpdateLog = updateLog;
      }
    } catch (Exception e) {
      IOUtils.closeQuietly(ourUpdateLog);
      ObjectReleaseTracker.release(this);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      ulog = ourUpdateLog;
    }
  }

  /**
   * Called when the Writer should be opened again - eg when replication replaces
   * all of the index files.
   *
   * @param rollback IndexWriter if true else close
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void newIndexWriter(boolean rollback) throws IOException;

  public abstract SolrCoreState getSolrCoreState();

  public abstract int addDoc(AddUpdateCommand cmd) throws IOException;
  public abstract void delete(DeleteUpdateCommand cmd) throws IOException;
  public abstract void deleteByQuery(DeleteUpdateCommand cmd) throws IOException;
  public abstract int mergeIndexes(MergeIndexesCommand cmd) throws IOException;
  public abstract void commit(CommitUpdateCommand cmd) throws IOException;
  public abstract void rollback(RollbackUpdateCommand cmd) throws IOException;
  public abstract UpdateLog getUpdateLog();

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

  public abstract void split(SplitIndexCommand cmd) throws IOException;

  @Override
  public Category getCategory() {
    return Category.UPDATE;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }
}
