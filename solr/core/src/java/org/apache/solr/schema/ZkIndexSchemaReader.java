package org.apache.solr.schema;
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

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

/** Keeps a ManagedIndexSchema up-to-date when changes are made to the serialized managed schema in ZooKeeper */
public class ZkIndexSchemaReader implements OnReconnect {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ManagedIndexSchemaFactory managedIndexSchemaFactory;
  private SolrZkClient zkClient;
  private String managedSchemaPath;

  public ZkIndexSchemaReader(ManagedIndexSchemaFactory managedIndexSchemaFactory) {
    this.managedIndexSchemaFactory = managedIndexSchemaFactory;
    ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)managedIndexSchemaFactory.getResourceLoader();
    this.zkClient = zkLoader.getZkController().getZkClient();
    managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + managedIndexSchemaFactory.getManagedSchemaResourceName();
    createSchemaWatcher();
    zkLoader.getZkController().addOnReconnectListener(this);
  }

  public Object getSchemaUpdateLock() { 
    return managedIndexSchemaFactory.getSchemaUpdateLock(); 
  }

  public void createSchemaWatcher() {
    log.info("Creating ZooKeeper watch for the managed schema at " + managedSchemaPath);

    try {
      zkClient.exists(managedSchemaPath, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          // session events are not change events, and do not remove the watcher
          if (Event.EventType.None.equals(event.getType())) {
            return;
          }
          log.info("A schema change: {}, has occurred - updating schema from ZooKeeper ...", event);
          try {
            updateSchema(this, -1);
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
      }, true);
    } catch (KeeperException e) {
      final String msg = "Error creating ZooKeeper watch for the managed schema";
      log.error(msg, e);
      throw new ZooKeeperException(ErrorCode.SERVER_ERROR, msg, e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("", e);
    }
  }

  public ManagedIndexSchema refreshSchemaFromZk(int expectedZkVersion) throws KeeperException, InterruptedException {
    updateSchema(null, expectedZkVersion);
    return managedIndexSchemaFactory.getSchema();
  }

  private void updateSchema(Watcher watcher, int expectedZkVersion) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    synchronized (getSchemaUpdateLock()) {
      final ManagedIndexSchema oldSchema = managedIndexSchemaFactory.getSchema();
      if (expectedZkVersion == -1 || oldSchema.schemaZkVersion < expectedZkVersion) {
        byte[] data = zkClient.getData(managedSchemaPath, watcher, stat, true);
        if (stat.getVersion() != oldSchema.schemaZkVersion) {
          log.info("Retrieved schema version "+ stat.getVersion() + " from ZooKeeper");
          long start = System.nanoTime();
          InputSource inputSource = new InputSource(new ByteArrayInputStream(data));
          String resourceName = managedIndexSchemaFactory.getManagedSchemaResourceName();
          ManagedIndexSchema newSchema = new ManagedIndexSchema
              (managedIndexSchemaFactory.getConfig(), resourceName, inputSource, managedIndexSchemaFactory.isMutable(), 
                  resourceName, stat.getVersion(), oldSchema.getSchemaUpdateLock());
          managedIndexSchemaFactory.setSchema(newSchema);
          long stop = System.nanoTime();
          log.info("Finished refreshing schema in " + TimeUnit.MILLISECONDS.convert(stop - start, TimeUnit.NANOSECONDS) + " ms");
        } else {
          log.info("Current schema version "+oldSchema.schemaZkVersion+" is already the latest");
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
      createSchemaWatcher();
      // force update now as the schema may have changed while our zk session was expired
      updateSchema(null, -1);
    } catch (Exception exc) {
      log.error("Failed to update managed-schema watcher after session expiration due to: "+exc, exc);
    }
  }
}
