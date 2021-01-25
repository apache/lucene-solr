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

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/** Keeps a ManagedIndexSchema up-to-date when changes are made to the serialized managed schema in ZooKeeper */
public class ZkIndexSchemaReader implements OnReconnect {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ManagedIndexSchemaFactory managedIndexSchemaFactory;
  private volatile SolrZkClient zkClient;
  private final String managedSchemaPath;
  private final String uniqueCoreId; // used in equals impl to uniquely identify the core that we're dependent on
  private final String collection;
  private volatile SchemaWatcher schemaWatcher;

  public ZkIndexSchemaReader(ManagedIndexSchemaFactory managedIndexSchemaFactory, SolrCore solrCore) throws KeeperException, InterruptedException {
    this.managedIndexSchemaFactory = managedIndexSchemaFactory;
    ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)managedIndexSchemaFactory.getResourceLoader();
    this.zkClient = solrCore.getCoreContainer().getZkController().getZkClient();
    this.managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + managedIndexSchemaFactory.getManagedSchemaResourceName();
    this.uniqueCoreId = solrCore.getName()+":"+solrCore.getStartNanoTime();
    this.collection = solrCore.getCoreDescriptor().getCollectionName();

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
        IOUtils.closeQuietly(schemaWatcher);
      //  schemaWatcher = null;
     //   ZkIndexSchemaReader.this.managedIndexSchemaFactory = null;
     //   zkClient = null;

      }
    });

    updateSchema();

    solrCore.getCoreContainer().getZkController().addOnReconnectListener(this);
  }

  public ReentrantLock getSchemaUpdateLock() {
    return managedIndexSchemaFactory.getSchemaUpdateLock(); 
  }

  public ManagedIndexSchema getSchema() {
    return managedIndexSchemaFactory.getSchema();
  }

  /**
   * Creates a schema watcher and returns it for controlling purposes.
   *
   */
  public void createSchemaWatcher() {
    if (log.isDebugEnabled()) log.debug("Creating ZooKeeper watch for the managed schema at {}", managedSchemaPath);
    IOUtils.closeQuietly(schemaWatcher);
    schemaWatcher = new SchemaWatcher(this);
  }
  
  /**
   * Watches for schema changes and triggers updates in the {@linkplain ZkIndexSchemaReader}.
   */
  public static class SchemaWatcher implements Watcher, Closeable {
    private volatile ZkIndexSchemaReader schemaReader;

    public SchemaWatcher(ZkIndexSchemaReader reader) {
      this.schemaReader = reader;
    }

    @Override
    public void process(WatchedEvent event) {

      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      log.info("A schema change: {}, has occurred - updating schema from ZooKeeper ...", event);
      try {
        schemaReader.updateSchema();
      } catch (Exception e) {
        log.error("", e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        schemaReader.zkClient.getSolrZooKeeper().removeWatches(schemaReader.managedSchemaPath, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

//  public ManagedIndexSchema refreshSchemaFromZk(int expectedZkVersion) throws KeeperException, InterruptedException {
//    updateSchema(null);
//    return managedIndexSchemaFactory.getSchema();
//  }

  // package visibility for test purposes
  public IndexSchema updateSchema() throws KeeperException, InterruptedException {
    ManagedIndexSchema newSchema;
    ReentrantLock  lock = getSchemaUpdateLock();
    lock.lock();
    try {
      Stat stat = new Stat();
      createSchemaWatcher();
      Stat exists = zkClient.exists(managedSchemaPath, schemaWatcher, true);
      if (exists == null) {
        log.info("{} does not exist yet, watching ...}", managedSchemaPath);
        return null;
      }

      int existsVersion = exists.getVersion();
      int v;

      v = managedIndexSchemaFactory.getSchema().getSchemaZkVersion();

      if (log.isDebugEnabled()) log.debug("Retrieved schema version {} from Zookeeper, existing={} schema={}", existsVersion, v, managedIndexSchemaFactory.getSchema());

      if (v >= existsVersion) {
        if (log.isDebugEnabled()) log.debug("Old schema version {} is >= found version {}", v, existsVersion);

        return null;
      }

      long start = System.nanoTime();
      byte[] data = zkClient.getData(managedSchemaPath, this.schemaWatcher, stat, true);

      InputSource inputSource = new InputSource(new ByteArrayInputStream(data));
      String resourceName = managedIndexSchemaFactory.getManagedSchemaResourceName();
      newSchema = new ManagedIndexSchema(managedIndexSchemaFactory, collection, managedIndexSchemaFactory.getConfig(), resourceName, inputSource, managedIndexSchemaFactory.isMutable(), resourceName,
          stat.getVersion());
      managedIndexSchemaFactory.setSchema(newSchema);

      long stop = System.nanoTime();
      log.info("Finished refreshing schema in {} ms", TimeUnit.MILLISECONDS.convert(stop - start, TimeUnit.NANOSECONDS));
    } catch (Exception e) {
      log.error("Exception updating schema", e);
      return null;
    } finally {
      if (lock != null && lock.isHeldByCurrentThread()) lock.unlock();
    }
    return newSchema;
  }

  /**
   * Called after a ZooKeeper session expiration occurs; need to re-create the watcher and update the current
   * schema from ZooKeeper.
   */
  @Override
  public void command() {
    try {
      // force update now as the schema may have changed while our zk session was expired
      updateSchema();
    } catch (Exception exc) {
      log.error("Failed to update managed-schema watcher after session expiration due to: {}", exc);
    }
  }

  @Override
  public String getName() {
    return uniqueCoreId;
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
