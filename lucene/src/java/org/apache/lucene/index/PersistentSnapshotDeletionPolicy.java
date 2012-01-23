package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

/**
 * A {@link SnapshotDeletionPolicy} which adds a persistence layer so that
 * snapshots can be maintained across the life of an application. The snapshots
 * are persisted in a {@link Directory} and are committed as soon as
 * {@link #snapshot(String)} or {@link #release(String)} is called.
 * <p>
 * <b>NOTE:</b> this class receives a {@link Directory} to persist the data into
 * a Lucene index. It is highly recommended to use a dedicated directory (and on
 * stable storage as well) for persisting the snapshots' information, and not
 * reuse the content index directory, or otherwise conflicts and index
 * corruption will occur.
 * <p>
 * <b>NOTE:</b> you should call {@link #close()} when you're done using this
 * class for safety (it will close the {@link IndexWriter} instance used).
 */
public class PersistentSnapshotDeletionPolicy extends SnapshotDeletionPolicy {

  // Used to validate that the given directory includes just one document w/ the
  // given ID field. Otherwise, it's not a valid Directory for snapshotting.
  private static final String SNAPSHOTS_ID = "$SNAPSHOTS_DOC$";

  // The index writer which maintains the snapshots metadata
  private final IndexWriter writer;

  /**
   * Reads the snapshots information from the given {@link Directory}. This
   * method can be used if the snapshots information is needed, however you
   * cannot instantiate the deletion policy (because e.g., some other process
   * keeps a lock on the snapshots directory).
   */
  public static Map<String, String> readSnapshotsInfo(Directory dir) throws IOException {
    IndexReader r = DirectoryReader.open(dir);
    Map<String, String> snapshots = new HashMap<String, String>();
    try {
      int numDocs = r.numDocs();
      // index is allowed to have exactly one document or 0.
      if (numDocs == 1) {
        Document doc = r.document(r.maxDoc() - 1);
        if (doc.getField(SNAPSHOTS_ID) == null) {
          throw new IllegalStateException("directory is not a valid snapshots store!");
        }
        doc.removeField(SNAPSHOTS_ID);
        for (IndexableField f : doc) {
          snapshots.put(f.name(), f.stringValue());
        }
      } else if (numDocs != 0) {
        throw new IllegalStateException(
            "should be at most 1 document in the snapshots directory: " + numDocs);
      }
    } finally {
      r.close();
    }
    return snapshots;
  }
  
  /**
   * {@link PersistentSnapshotDeletionPolicy} wraps another
   * {@link IndexDeletionPolicy} to enable flexible snapshotting.
   * 
   * @param primary
   *          the {@link IndexDeletionPolicy} that is used on non-snapshotted
   *          commits. Snapshotted commits, by definition, are not deleted until
   *          explicitly released via {@link #release(String)}.
   * @param dir
   *          the {@link Directory} which will be used to persist the snapshots
   *          information.
   * @param mode
   *          specifies whether a new index should be created, deleting all
   *          existing snapshots information (immediately), or open an existing
   *          index, initializing the class with the snapshots information.
   * @param matchVersion
   *          specifies the {@link Version} that should be used when opening the
   *          IndexWriter.
   */
  public PersistentSnapshotDeletionPolicy(IndexDeletionPolicy primary,
      Directory dir, OpenMode mode, Version matchVersion)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    super(primary, null);

    // Initialize the index writer over the snapshot directory.
    writer = new IndexWriter(dir, new IndexWriterConfig(matchVersion, null).setOpenMode(mode));
    if (mode != OpenMode.APPEND) {
      // IndexWriter no longer creates a first commit on an empty Directory. So
      // if we were asked to CREATE*, call commit() just to be sure. If the
      // index contains information and mode is CREATE_OR_APPEND, it's a no-op.
      writer.commit();
    }

    try {
      // Initializes the snapshots information. This code should basically run
      // only if mode != CREATE, but if it is, it's no harm as we only open the
      // reader once and immediately close it.
      for (Entry<String, String> e : readSnapshotsInfo(dir).entrySet()) {
        registerSnapshotInfo(e.getKey(), e.getValue(), null);
      }
    } catch (RuntimeException e) {
      writer.close(); // don't leave any open file handles
      throw e;
    } catch (IOException e) {
      writer.close(); // don't leave any open file handles
      throw e;
    }
  }

  @Override
  public synchronized void onInit(List<? extends IndexCommit> commits)
  throws IOException {
    // super.onInit() needs to be called first to ensure that initialization
    // behaves as expected. The superclass, SnapshotDeletionPolicy, ensures
    // that any snapshot IDs with empty IndexCommits are released. Since this 
    // happens, this class needs to persist these changes.
    super.onInit(commits);
    persistSnapshotInfos(null, null);
  }

  /**
   * Snapshots the last commit using the given ID. Once this method returns, the
   * snapshot information is persisted in the directory.
   * 
   * @see SnapshotDeletionPolicy#snapshot(String)
   */
  @Override
  public synchronized IndexCommit snapshot(String id) throws IOException {
    checkSnapshotted(id);
    if (SNAPSHOTS_ID.equals(id)) {
      throw new IllegalArgumentException(id + " is reserved and cannot be used as a snapshot id");
    }
    persistSnapshotInfos(id, lastCommit.getSegmentsFileName());
    return super.snapshot(id);
  }

  /**
   * Deletes a snapshotted commit by ID. Once this method returns, the snapshot
   * information is committed to the directory.
   * 
   * @see SnapshotDeletionPolicy#release(String)
   */
  @Override
  public synchronized void release(String id) throws IOException {
    super.release(id);
    persistSnapshotInfos(null, null);
  }

  /** Closes the index which writes the snapshots to the directory. */
  public void close() throws CorruptIndexException, IOException {
    writer.close();
  }

  /**
   * Persists all snapshots information. If the given id and segment are not
   * null, it persists their information as well.
   */
  private void persistSnapshotInfos(String id, String segment) throws IOException {
    writer.deleteAll();
    Document d = new Document();
    FieldType ft = new FieldType();
    ft.setStored(true);
    d.add(new Field(SNAPSHOTS_ID, "", ft));
    for (Entry<String, String> e : super.getSnapshots().entrySet()) {
      d.add(new Field(e.getKey(), e.getValue(), ft));
    }
    if (id != null) {
      d.add(new Field(id, segment, ft));
    }
    writer.addDocument(d);
    writer.commit();
  }

}
