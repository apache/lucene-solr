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
package org.apache.solr.schema;
import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keeps a ManagedIndexSchema up-to-date when changes are made to the serialized managed schema in ZooKeeper */
public class ZkIndexSchemaReader implements OnReconnect {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ManagedIndexSchemaFactory managedIndexSchemaFactory;
  private SolrZkClient zkClient;
  private String managedSchemaPath;
  private final String uniqueCoreId; // used in equals impl to uniquely identify the core that we're dependent on
  private SchemaWatcher schemaWatcher;
  private ZkSolrResourceLoader zkLoader;

  public ZkIndexSchemaReader(ManagedIndexSchemaFactory managedIndexSchemaFactory, SolrCore solrCore) {
    this.managedIndexSchemaFactory = managedIndexSchemaFactory;
    zkLoader = (ZkSolrResourceLoader)managedIndexSchemaFactory.getResourceLoader();
    this.zkClient = zkLoader.getZkController().getZkClient();
    this.managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + managedIndexSchemaFactory.getManagedSchemaResourceName();
    this.uniqueCoreId = solrCore.getName()+":"+solrCore.getStartNanoTime();

    // register a CloseHook for the core this reader is linked to, so that we can de-register the listener
    solrCore.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        CoreContainer cc = core.getCoreContainer();
        if (cc.isZooKeeperAware()) {
          if (log.isDebugEnabled()) {
            log.debug("Removing ZkIndexSchemaReader OnReconnect listener as core {} is shutting down.", core.getName());
          }
          cc.getZkController().removeOnReconnectListener(ZkIndexSchemaReader.this);
        }
      }

      @Override
      public void postClose(SolrCore core) {
        // The watcher is still registered with Zookeeper, and holds a
        // reference to the schema reader, which indirectly references the
        // SolrCore and would prevent it from being garbage collected.
        schemaWatcher.discardReaderReference();
      }
    });

    this.schemaWatcher = createSchemaWatcher();

    zkLoader.getZkController().addOnReconnectListener(this);
  }

  public Object getSchemaUpdateLock() {
    return managedIndexSchemaFactory.getSchemaUpdateLock();
  }

  /**
   * Creates a schema watcher and returns it for controlling purposes.
   *
   * @return the registered {@linkplain SchemaWatcher}.
   */
  public SchemaWatcher createSchemaWatcher() {
    log.info("Creating ZooKeeper watch for the managed schema at {}", managedSchemaPath);

    SchemaWatcher watcher = new SchemaWatcher(this);
    try {
      zkClient.exists(managedSchemaPath, watcher, true);
    } catch (KeeperException e) {
      final String msg = "Error creating ZooKeeper watch for the managed schema";
      log.error(msg, e);
      throw new ZooKeeperException(ErrorCode.SERVER_ERROR, msg, e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("", e);
    }

    return watcher;
  }

  /**
   * Watches for schema changes and triggers updates in the {@linkplain ZkIndexSchemaReader}.
   */
  public static class SchemaWatcher implements Watcher {

    private ZkIndexSchemaReader schemaReader;

    public SchemaWatcher(ZkIndexSchemaReader reader) {
      this.schemaReader = reader;
    }

    @Override
    public void process(WatchedEvent event) {
      ZkIndexSchemaReader indexSchemaReader = schemaReader;

      if (indexSchemaReader == null) {
        return; // the core for this reader has already been removed, don't process this event
      }

      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      log.info("A schema change: {}, has occurred - updating schema from ZooKeeper ...", event);
      try {
        indexSchemaReader.updateSchema(this, -1);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.SESSIONEXPIRED || e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
          return;
        }
        log.error("", e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("", e);
      }
    }

    /**
     * Discard the reference to the {@code ZkIndexSchemaReader}.
     */
    public void discardReaderReference() {
      schemaReader = null;
    }
  }

  public ManagedIndexSchema refreshSchemaFromZk(int expectedZkVersion) throws KeeperException, InterruptedException {
    updateSchema(null, expectedZkVersion);
    return managedIndexSchemaFactory.getSchema();
  }

  // package visibility for test purposes
  void updateSchema(Watcher watcher, int expectedZkVersion) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    synchronized (getSchemaUpdateLock()) {
      final ManagedIndexSchema oldSchema = managedIndexSchemaFactory.getSchema();
      if (expectedZkVersion == -1 || oldSchema.schemaZkVersion < expectedZkVersion) {
        byte[] data = zkClient.getData(managedSchemaPath, watcher, stat, true);
        if (stat.getVersion() != oldSchema.schemaZkVersion) {
          if (log.isInfoEnabled()) {
            log.info("Retrieved schema version {} from Zookeeper", stat.getVersion());
          }
          long start = System.nanoTime();
          String resourceName = managedIndexSchemaFactory.getManagedSchemaResourceName();
          ManagedIndexSchema newSchema = new ManagedIndexSchema
                  (managedIndexSchemaFactory.getConfig(), resourceName,
                          () -> ConfigSetService.getParsedSchema(new ByteArrayInputStream(data),zkLoader , resourceName), managedIndexSchemaFactory.isMutable(),
                          resourceName, stat.getVersion(), oldSchema.getSchemaUpdateLock());
          managedIndexSchemaFactory.setSchema(newSchema);
          long stop = System.nanoTime();
          log.info("Finished refreshing schema in {} ms", TimeUnit.MILLISECONDS.convert(stop - start, TimeUnit.NANOSECONDS));
        } else {
          log.info("Current schema version {} is already the latest", oldSchema.schemaZkVersion);
        }
      }
    }
  }

  /**
   * Called after a ZooKeeper session expiration occurs; need to re-create the watcher and update the current
   * schema from ZooKeeper.
   */
  @Override
  public void command() {
    try {
      // setup a new watcher to get notified when the managed schema changes
      schemaWatcher = createSchemaWatcher();
      // force update now as the schema may have changed while our zk session was expired
      updateSchema(null, -1);
    } catch (Exception exc) {
      log.error("Failed to update managed-schema watcher after session expiration due to: {}", exc);
    }
  }

  public String getUniqueCoreId() {
    return uniqueCoreId;
  }

  public String toString() {
    return "ZkIndexSchemaReader: "+managedSchemaPath+", uniqueCoreId: "+uniqueCoreId;
  }

  public int hashCode() {
    return managedSchemaPath.hashCode()+uniqueCoreId.hashCode();
  }

  // We need the uniqueCoreId which is core name + start time nanos to be the tie breaker
  // as there can be multiple ZkIndexSchemaReader instances active for the same core after
  // a reload (one is initializing and the other is being shutdown)
  public boolean equals(Object other) {
    if (other == null) return false;
    if (other == this) return true;
    if (!(other instanceof ZkIndexSchemaReader)) return false;
    ZkIndexSchemaReader that = (ZkIndexSchemaReader)other;
    return this.managedSchemaPath.equals(that.managedSchemaPath) && this.uniqueCoreId.equals(that.uniqueCoreId);
  }
}
