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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides functionality required to handle the data files corresponding to Solr snapshots.
 */
public class SolrSnapshotManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
}
