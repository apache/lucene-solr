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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
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
   * @param dir The index directory storing the snapshot.
   * @param gen The generation number for the {@linkplain IndexCommit}
   * @throws IOException in case of I/O errors.
   */
  public static void deleteIndexFiles ( Directory dir, Collection<SnapshotMetaData> snapshots, long gen ) throws IOException {
    List<IndexCommit> commits = DirectoryReader.listCommits(dir);
    Map<String, Integer> refCounts = buildRefCounts(snapshots, commits);
    for (IndexCommit ic : commits) {
      if (ic.getGeneration() == gen) {
        deleteIndexFiles(dir,refCounts, ic);
        break;
      }
    }
  }

  /**
   * This method deletes all files not corresponding to a configured snapshot in the specified index directory.
   *
   * @param dir The index directory to search for.
   * @throws IOException in case of I/O errors.
   */
  public static void deleteNonSnapshotIndexFiles (Directory dir, Collection<SnapshotMetaData> snapshots) throws IOException {
    List<IndexCommit> commits = DirectoryReader.listCommits(dir);
    Map<String, Integer> refCounts = buildRefCounts(snapshots, commits);
    Set<Long> snapshotGenNumbers = snapshots.stream()
                                            .map(SnapshotMetaData::getGenerationNumber)
                                            .collect(Collectors.toSet());
    for (IndexCommit ic : commits) {
      if (!snapshotGenNumbers.contains(ic.getGeneration())) {
        deleteIndexFiles(dir,refCounts, ic);
      }
    }
  }

  /**
   * This method computes reference count for the index files by taking into consideration
   * (a) configured snapshots and (b) files sharing between two or more {@linkplain IndexCommit} instances.
   *
   * @param snapshots A collection of user configured snapshots
   * @param commits A list of {@linkplain IndexCommit} instances
   * @return A map containing reference count for each index file referred in one of the {@linkplain IndexCommit} instances.
   * @throws IOException in case of I/O error.
   */
  @VisibleForTesting
  static Map<String, Integer> buildRefCounts (Collection<SnapshotMetaData> snapshots, List<IndexCommit> commits) throws IOException {
    Map<String, Integer> result = new HashMap<>();
    Map<Long, IndexCommit> commitsByGen = commits.stream().collect(
        Collectors.toMap(IndexCommit::getGeneration, Function.identity()));

    for(SnapshotMetaData md : snapshots) {
      IndexCommit ic = commitsByGen.get(md.getGenerationNumber());
      if (ic != null) {
        Collection<String> fileNames = ic.getFileNames();
        for(String fileName : fileNames) {
          int refCount = result.getOrDefault(fileName, 0);
          result.put(fileName, refCount+1);
        }
      }
    }

    return result;
  }

  /**
   * This method deletes the index files associated with specified <code>indexCommit</code> provided they
   * are not referred by some other {@linkplain IndexCommit}.
   *
   * @param dir The index directory containing the {@linkplain IndexCommit} to be deleted.
   * @param refCounts A map containing reference counts for each file associated with every {@linkplain IndexCommit}
   *                  in the specified directory.
   * @param indexCommit The {@linkplain IndexCommit} whose files need to be deleted.
   * @throws IOException in case of I/O errors.
   */
  private static void deleteIndexFiles ( Directory dir, Map<String, Integer> refCounts, IndexCommit indexCommit ) throws IOException {
    log.info("Deleting index files for index commit with generation {} in directory {}", indexCommit.getGeneration(), dir);
    for (String fileName : indexCommit.getFileNames()) {
      try {
        // Ensure that a file being deleted is not referred by some other commit.
        int ref = refCounts.getOrDefault(fileName, 0);
        log.debug("Reference count for file {} is {}", fileName, ref);
        if (ref == 0) {
          dir.deleteFile(fileName);
        }
      } catch (IOException e) {
        log.warn("Unable to delete file {} in directory {} due to exception {}", fileName, dir, e.getMessage());
      }
    }
  }
}
