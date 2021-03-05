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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible to manage the persistent snapshots meta-data for the Solr indexes. The
 * persistent snapshots are implemented by relying on Lucene {@linkplain IndexDeletionPolicy}
 * abstraction to configure a specific {@linkplain IndexCommit} to be retained. The
 * {@linkplain IndexDeletionPolicyWrapper} in Solr uses this class to create/delete the Solr index
 * snapshots.
 */
public class SolrSnapshotMetaDataManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String SNAPSHOT_METADATA_DIR = "snapshot_metadata";

  /**
   * A class defining the meta-data for a specific snapshot.
   */
  public static class SnapshotMetaData {
    private String name;
    private String indexDirPath;
    private long generationNumber;

    public SnapshotMetaData(String name, String indexDirPath, long generationNumber) {
      super();
      this.name = name;
      this.indexDirPath = indexDirPath;
      this.generationNumber = generationNumber;
    }

    public String getName() {
      return name;
    }

    public String getIndexDirPath() {
      return indexDirPath;
    }

    public long getGenerationNumber() {
      return generationNumber;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("SnapshotMetaData[name=");
      builder.append(name);
      builder.append(", indexDirPath=");
      builder.append(indexDirPath);
      builder.append(", generation=");
      builder.append(generationNumber);
      builder.append("]");
      return builder.toString();
    }
  }

  /** Prefix used for the save file. */
  public static final String SNAPSHOTS_PREFIX = "snapshots_";
  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = VERSION_START;
  private static final String CODEC_NAME = "solr-snapshots";

  // The index writer which maintains the snapshots metadata
  private long nextWriteGen;

  private final Directory dir;

  /** Used to map snapshot name to snapshot meta-data. */
  protected final Map<String,SnapshotMetaData> nameToDetailsMapping = new LinkedHashMap<>();
  /** Used to figure out the *current* index data directory path */
  private final SolrCore solrCore;

  /**
   * A constructor.
   *
   * @param dir The directory where the snapshot meta-data should be stored. Enables updating
   *            the existing meta-data.
   * @throws IOException in case of errors.
   */
  public SolrSnapshotMetaDataManager(SolrCore solrCore, Directory dir) throws IOException {
    this(solrCore, dir, OpenMode.CREATE_OR_APPEND);
  }

  /**
   * A constructor.
   *
   * @param dir The directory where the snapshot meta-data is stored.
   * @param mode CREATE If previous meta-data should be erased.
   *             APPEND If previous meta-data should be read and updated.
   *             CREATE_OR_APPEND Creates a new meta-data structure if one does not exist
   *                              Updates the existing structure if one exists.
   * @throws IOException in case of errors.
   */
  public SolrSnapshotMetaDataManager(SolrCore solrCore, Directory dir, OpenMode mode) throws IOException {
    this.solrCore = solrCore;
    this.dir = dir;

    if (mode == OpenMode.CREATE) {
      deleteSnapshotMetadataFiles();
    }

    loadFromSnapshotMetadataFile();

    if (mode == OpenMode.APPEND && nextWriteGen == 0) {
      throw new IllegalStateException("no snapshots stored in this directory");
    }
  }

  /**
   * @return The snapshot meta-data directory
   */
  public Directory getSnapshotsDir() {
    return dir;
  }

  /**
   * This method creates a new snapshot meta-data entry.
   *
   * @param name The name of the snapshot.
   * @param indexDirPath The directory path where the index files are stored.
   * @param gen The generation number for the {@linkplain IndexCommit} being snapshotted.
   * @throws IOException in case of I/O errors.
   */
  public synchronized void snapshot(String name, String indexDirPath, long gen) throws IOException {
    Objects.requireNonNull(name);

    if (log.isInfoEnabled()) {
      log.info("Creating the snapshot named {} for core {} associated with index commit with generation {} in directory {}"
          , name, solrCore.getName(), gen, indexDirPath);
    }

    if(nameToDetailsMapping.containsKey(name)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "A snapshot with name " + name + " already exists");
    }

    SnapshotMetaData d = new SnapshotMetaData(name, indexDirPath, gen);
    nameToDetailsMapping.put(name, d);

    boolean success = false;
    try {
      persist();
      success = true;
    } finally {
      if (!success) {
        try {
          release(name);
        } catch (Exception e) {
          // Suppress so we keep throwing original exception
        }
      }
    }
  }

  /**
   * This method deletes a previously created snapshot (if any).
   *
   * @param name The name of the snapshot to be deleted.
   * @return The snapshot meta-data if the snapshot with the snapshot name exists.
   * @throws IOException in case of I/O error
   */
  public synchronized Optional<SnapshotMetaData> release(String name) throws IOException {
    if (log.isInfoEnabled()) {
      log.info("Deleting the snapshot named {} for core {}", name, solrCore.getName());
    }
    SnapshotMetaData result = nameToDetailsMapping.remove(Objects.requireNonNull(name));
    if(result != null) {
      boolean success = false;
      try {
        persist();
        success = true;
      } finally {
        if (!success) {
          nameToDetailsMapping.put(name, result);
        }
      }
    }
    return Optional.ofNullable(result);
  }

  /**
   * This method returns if snapshot is created for the specified generation number in
   * the *current* index directory.
   *
   * @param genNumber The generation number for the {@linkplain IndexCommit} to be checked.
   * @return true if the snapshot is created.
   *         false otherwise.
   */
  public synchronized boolean isSnapshotted(long genNumber) {
    return !nameToDetailsMapping.isEmpty() && isSnapshotted(solrCore.getIndexDir(), genNumber);
  }

  /**
   * This method returns if snapshot is created for the specified generation number in
   * the specified index directory.
   *
   * @param genNumber The generation number for the {@linkplain IndexCommit} to be checked.
   * @return true if the snapshot is created.
   *         false otherwise.
   */
  public synchronized boolean isSnapshotted(String indexDirPath, long genNumber) {
    return !nameToDetailsMapping.isEmpty()
        && nameToDetailsMapping.values().stream()
           .anyMatch(entry -> entry.getIndexDirPath().equals(indexDirPath) && entry.getGenerationNumber() == genNumber);
  }

  /**
   * This method returns the snapshot meta-data for the specified name (if it exists).
   *
   * @param name The name of the snapshot
   * @return The snapshot meta-data if exists.
   */
  public synchronized Optional<SnapshotMetaData> getSnapshotMetaData(String name) {
    return Optional.ofNullable(nameToDetailsMapping.get(name));
  }

  /**
   * @return A list of snapshots created so far.
   */
  public synchronized List<String> listSnapshots() {
    // We create a copy for thread safety.
    return new ArrayList<>(nameToDetailsMapping.keySet());
  }

  /**
   * This method returns a list of snapshots created in a specified index directory.
   *
   * @param indexDirPath The index directory path.
   * @return a list snapshots stored in the specified directory.
   */
  public synchronized Collection<SnapshotMetaData> listSnapshotsInIndexDir(String indexDirPath) {
    return nameToDetailsMapping.values().stream()
        .filter(entry -> indexDirPath.equals(entry.getIndexDirPath()))
        .collect(Collectors.toList());
  }

  /**
   * This method returns the {@linkplain IndexCommit} associated with the specified
   * <code>commitName</code>. A snapshot with specified <code>commitName</code> must
   * be created before invoking this method.
   *
   * @param commitName The name of persisted commit
   * @return the {@linkplain IndexCommit}
   * @throws IOException in case of I/O error.
   */
  public Optional<IndexCommit> getIndexCommitByName(String commitName) throws IOException {
    Optional<IndexCommit> result = Optional.empty();
    Optional<SnapshotMetaData> metaData = getSnapshotMetaData(commitName);
    if (metaData.isPresent()) {
      String indexDirPath = metaData.get().getIndexDirPath();
      long gen = metaData.get().getGenerationNumber();

      Directory d = solrCore.getDirectoryFactory().get(indexDirPath, DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NONE);
      try {
        result = DirectoryReader.listCommits(d)
                                .stream()
                                .filter(ic -> ic.getGeneration() == gen)
                                .findAny();

        if (!result.isPresent()) {
          log.warn("Unable to find commit with generation {} in the directory {}", gen, indexDirPath);
        }

      } finally {
        solrCore.getDirectoryFactory().release(d);
      }
    } else {
      log.warn("Commit with name {} is not persisted for core {}", commitName, solrCore.getName());
    }

    return result;
  }

  private synchronized void persist() throws IOException {
    String fileName = SNAPSHOTS_PREFIX + nextWriteGen;
    IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT);
    boolean success = false;
    try {
      CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
      out.writeVInt(nameToDetailsMapping.size());
      for(Entry<String,SnapshotMetaData> ent : nameToDetailsMapping.entrySet()) {
        out.writeString(ent.getKey());
        out.writeString(ent.getValue().getIndexDirPath());
        out.writeVLong(ent.getValue().getGenerationNumber());
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(out);
        IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
      } else {
        IOUtils.close(out);
      }
    }

    dir.sync(Collections.singletonList(fileName));

    if (nextWriteGen > 0) {
      String lastSaveFile = SNAPSHOTS_PREFIX + (nextWriteGen-1);
      // exception OK: likely it didn't exist
      IOUtils.deleteFilesIgnoringExceptions(dir, lastSaveFile);
    }

    nextWriteGen++;
  }

  private synchronized void deleteSnapshotMetadataFiles() throws IOException {
    for(String file : dir.listAll()) {
      if (file.startsWith(SNAPSHOTS_PREFIX)) {
        dir.deleteFile(file);
      }
    }
  }

  /**
   * Reads the snapshot meta-data information from the given {@link Directory}.
   */
  private synchronized void loadFromSnapshotMetadataFile() throws IOException {
    log.debug("Loading from snapshot metadata file...");
    long genLoaded = -1;
    IOException ioe = null;
    List<String> snapshotFiles = new ArrayList<>();
    for(String file : dir.listAll()) {
      if (file.startsWith(SNAPSHOTS_PREFIX)) {
        long gen = Long.parseLong(file.substring(SNAPSHOTS_PREFIX.length()));
        if (genLoaded == -1 || gen > genLoaded) {
          snapshotFiles.add(file);
          Map<String, SnapshotMetaData> snapshotMetaDataMapping = new HashMap<>();
          IndexInput in = dir.openInput(file, IOContext.DEFAULT);
          try {
            CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_START);
            int count = in.readVInt();
            for(int i=0;i<count;i++) {
              String name = in.readString();
              String indexDirPath = in.readString();
              long commitGen = in.readVLong();
              snapshotMetaDataMapping.put(name, new SnapshotMetaData(name, indexDirPath, commitGen));
            }
          } catch (IOException ioe2) {
            // Save first exception & throw in the end
            if (ioe == null) {
              ioe = ioe2;
            }
          } finally {
            in.close();
          }

          genLoaded = gen;
          nameToDetailsMapping.clear();
          nameToDetailsMapping.putAll(snapshotMetaDataMapping);
        }
      }
    }

    if (genLoaded == -1) {
      // Nothing was loaded...
      if (ioe != null) {
        // ... not for lack of trying:
        throw ioe;
      }
    } else {
      if (snapshotFiles.size() > 1) {
        // Remove any broken / old snapshot files:
        String curFileName = SNAPSHOTS_PREFIX + genLoaded;
        for(String file : snapshotFiles) {
          if (!curFileName.equals(file)) {
            IOUtils.deleteFilesIgnoringExceptions(dir, file);
          }
        }
      }
      nextWriteGen = 1+genLoaded;
    }
  }
}
