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
package org.apache.solr.core.snapshots;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides functionality required to handle the data files corresponding to Solr snapshots.
 */
public class SolrSnapshotManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String INDEX_DIR_PATH = "indexDirPath";
  public static final String GENERATION_NUM = "generation";
  public static final String SNAPSHOT_STATUS = "status";
  public static final String CREATION_DATE = "creationDate";
  public static final String SNAPSHOT_REPLICAS = "replicas";
  public static final String SNAPSHOTS_INFO = "snapshots";
  public static final String LEADER = "leader";
  public static final String SHARD_ID = "shard_id";
  public static final String FILE_LIST = "files";

  /**
   * This method returns if a named snapshot exists for the specified collection.
   *
   * @param zkClient Zookeeper client
   * @param collectionName The name of the collection
   * @param commitName The name of the snapshot
   * @return true if the named snapshot exists
   *         false Otherwise
   * @throws KeeperException In case of Zookeeper error
   * @throws InterruptedException In case of thread interruption.
   */
  public static boolean snapshotExists(SolrZkClient zkClient, String collectionName, String commitName)
      throws KeeperException, InterruptedException {
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.ofNullable(commitName));
    return zkClient.exists(zkPath, true);
  }

  /**
   * This method creates an entry for the named snapshot for the specified collection in Zookeeper.
   *
   * @param zkClient Zookeeper client
   * @param collectionName The name of the collection
   * @param meta The {@linkplain CollectionSnapshotMetaData} corresponding to named snapshot
   * @throws KeeperException In case of Zookeeper error
   * @throws InterruptedException In case of thread interruption.
   */
  public static void createCollectionLevelSnapshot(SolrZkClient zkClient, String collectionName,
      CollectionSnapshotMetaData meta) throws KeeperException, InterruptedException {
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.of(meta.getName()));
    zkClient.makePath(zkPath, Utils.toJSON(meta), CreateMode.PERSISTENT, true);
  }

  /**
   * This method updates an entry for the named snapshot for the specified collection in Zookeeper.
   *
   * @param zkClient Zookeeper client
   * @param collectionName  The name of the collection
   * @param meta The {@linkplain CollectionSnapshotMetaData} corresponding to named snapshot
   * @throws KeeperException In case of Zookeeper error
   * @throws InterruptedException In case of thread interruption.
   */
  public static void updateCollectionLevelSnapshot(SolrZkClient zkClient, String collectionName,
      CollectionSnapshotMetaData meta) throws KeeperException, InterruptedException {
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.of(meta.getName()));
    zkClient.setData(zkPath, Utils.toJSON(meta), -1, true);
  }

  /**
   * This method deletes an entry for the named snapshot for the specified collection in Zookeeper.
   *
   * @param zkClient Zookeeper client
   * @param collectionName The name of the collection
   * @param commitName  The name of the snapshot
   * @throws InterruptedException In case of thread interruption.
   * @throws KeeperException  In case of Zookeeper error
   */
  public static void deleteCollectionLevelSnapshot(SolrZkClient zkClient, String collectionName, String commitName)
      throws InterruptedException, KeeperException {
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.of(commitName));
    zkClient.delete(zkPath, -1, true);
  }

  /**
   * This method deletes all snapshots for the specified collection in Zookeeper.
   *
   * @param zkClient  Zookeeper client
   * @param collectionName The name of the collection
   * @throws InterruptedException In case of thread interruption.
   * @throws KeeperException In case of Zookeeper error
   */
  public static void cleanupCollectionLevelSnapshots(SolrZkClient zkClient, String collectionName)
      throws InterruptedException, KeeperException {
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.empty());
    try {
      // Delete the meta-data for each snapshot.
      Collection<String> snapshots = zkClient.getChildren(zkPath, null, true);
      for (String snapshot : snapshots) {
        String path = getSnapshotMetaDataZkPath(collectionName, Optional.of(snapshot));
        try {
          zkClient.delete(path, -1, true);
        } catch (KeeperException ex) {
          // Gracefully handle the case when the zk node doesn't exist
          if ( ex.code() != KeeperException.Code.NONODE ) {
            throw ex;
          }
        }
      }

      // Delete the parent node.
      zkClient.delete(zkPath, -1, true);
    } catch (KeeperException ex) {
      // Gracefully handle the case when the zk node doesn't exist (e.g. if no snapshots were created for this collection).
      if ( ex.code() != KeeperException.Code.NONODE ) {
        throw ex;
      }
    }
  }

  /**
   * This method returns the {@linkplain CollectionSnapshotMetaData} for the named snapshot for the specified collection in Zookeeper.
   *
   * @param zkClient  Zookeeper client
   * @param collectionName  The name of the collection
   * @param commitName The name of the snapshot
   * @return (Optional) the {@linkplain CollectionSnapshotMetaData}
   * @throws InterruptedException In case of thread interruption.
   * @throws KeeperException In case of Zookeeper error
   */
  public static Optional<CollectionSnapshotMetaData> getCollectionLevelSnapshot(SolrZkClient zkClient, String collectionName, String commitName)
      throws InterruptedException, KeeperException {
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.of(commitName));
    try {
      Map<String, Object> data = (Map<String, Object>)Utils.fromJSON(zkClient.getData(zkPath, null, null, true));
      return Optional.of(new CollectionSnapshotMetaData(data));
    } catch (KeeperException ex) {
      // Gracefully handle the case when the zk node for a specific
      // snapshot doesn't exist (e.g. due to a concurrent delete operation).
      if ( ex.code() == KeeperException.Code.NONODE ) {
        return Optional.empty();
      }
      throw ex;
    }
  }

  /**
   * This method returns the {@linkplain CollectionSnapshotMetaData} for each named snapshot for the specified collection in Zookeeper.
   *
   * @param zkClient Zookeeper client
   * @param collectionName The name of the collection
   * @return the {@linkplain CollectionSnapshotMetaData} for each named snapshot
   * @throws InterruptedException In case of thread interruption.
   * @throws KeeperException In case of Zookeeper error
   */
  public static Collection<CollectionSnapshotMetaData> listSnapshots(SolrZkClient zkClient, String collectionName)
      throws InterruptedException, KeeperException {
    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    String zkPath = getSnapshotMetaDataZkPath(collectionName, Optional.empty());

    try {
      Collection<String> snapshots = zkClient.getChildren(zkPath, null, true);
      for (String snapshot : snapshots) {
        Optional<CollectionSnapshotMetaData> s = getCollectionLevelSnapshot(zkClient, collectionName, snapshot);
        if (s.isPresent()) {
          result.add(s.get());
        }
      }
    } catch (KeeperException ex) {
      // Gracefully handle the case when the zk node doesn't exist (e.g. due to a concurrent delete collection operation).
      if ( ex.code() != KeeperException.Code.NONODE ) {
        throw ex;
      }
    }
    return result;
  }


  /**
   * This method deletes index files of the {@linkplain IndexCommit} for the specified generation number.
   *
   * @param core The Solr core
   * @param dir The index directory storing the snapshot.
   * @param gen The generation number of the {@linkplain IndexCommit} to be deleted.
   * @throws IOException in case of I/O errors.
   */
  public static void deleteSnapshotIndexFiles(SolrCore core, Directory dir, final long gen) throws IOException {
    deleteSnapshotIndexFiles(core, dir, new IndexDeletionPolicy() {
      @Override
      public void onInit(List<? extends IndexCommit> commits) throws IOException {
        for (IndexCommit ic : commits) {
          if (gen == ic.getGeneration()) {
            log.info("Deleting non-snapshotted index commit with generation {}", ic.getGeneration());
            ic.delete();
          }
        }
      }

      @Override
      public void onCommit(List<? extends IndexCommit> commits)
          throws IOException {}
    });
  }

  /**
   * This method deletes index files not associated with the specified <code>snapshots</code>.
   *
   * @param core The Solr core
   * @param dir The index directory storing the snapshot.
   * @param snapshots The snapshots to be preserved.
   * @throws IOException in case of I/O errors.
   */
  public static void deleteNonSnapshotIndexFiles(SolrCore core, Directory dir, Collection<SnapshotMetaData> snapshots) throws IOException {
    final Set<Long> genNumbers = new HashSet<>();
    for (SnapshotMetaData m : snapshots) {
      genNumbers.add(m.getGenerationNumber());
    }

    deleteSnapshotIndexFiles(core, dir, new IndexDeletionPolicy() {
      @Override
      public void onInit(List<? extends IndexCommit> commits) throws IOException {
        for (IndexCommit ic : commits) {
          if (!genNumbers.contains(ic.getGeneration())) {
            log.info("Deleting non-snapshotted index commit with generation {}", ic.getGeneration());
            ic.delete();
          }
        }
      }

      @Override
      public void onCommit(List<? extends IndexCommit> commits)
          throws IOException {}
    });
  }

  /**
   * This method deletes index files of the {@linkplain IndexCommit} for the specified generation number.
   *
   * @param core The Solr core
   * @param dir The index directory storing the snapshot.
   * @throws IOException in case of I/O errors.
   */
  private static void deleteSnapshotIndexFiles(SolrCore core, Directory dir, IndexDeletionPolicy delPolicy) throws IOException {
    IndexWriterConfig conf = core.getSolrConfig().indexConfig.toIndexWriterConfig(core);
    conf.setOpenMode(OpenMode.APPEND);
    conf.setMergePolicy(NoMergePolicy.INSTANCE);//Don't want to merge any commits here!
    conf.setIndexDeletionPolicy(delPolicy);
    conf.setCodec(core.getCodec());

    try (SolrIndexWriter iw = new SolrIndexWriter("SolrSnapshotCleaner", dir, conf)) {
      // Do nothing. The only purpose of opening index writer is to invoke the Lucene IndexDeletionPolicy#onInit
      // method so that we can cleanup the files associated with specified index commit.
      // Note the index writer creates a new commit during the close() operation (which is harmless).
    }
  }

  private static String getSnapshotMetaDataZkPath(String collectionName, Optional<String> commitName) {
    if (commitName.isPresent()) {
      return "/snapshots/"+collectionName+"/"+commitName.get();
    }
    return "/snapshots/"+collectionName;
  }
}
