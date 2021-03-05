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
package org.apache.lucene.replicator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * A {@link Revision} of a single index and taxonomy index files which comprises the list of files
 * from both indexes. This revision should be used whenever a pair of search and taxonomy indexes
 * need to be replicated together to guarantee consistency of both on the replicating (client) side.
 *
 * @see IndexRevision
 * @lucene.experimental
 */
public class IndexAndTaxonomyRevision implements Revision {

  /**
   * A {@link DirectoryTaxonomyWriter} which sets the underlying {@link IndexWriter}'s {@link
   * IndexDeletionPolicy} to {@link SnapshotDeletionPolicy}.
   */
  public static final class SnapshotDirectoryTaxonomyWriter extends DirectoryTaxonomyWriter {

    private SnapshotDeletionPolicy sdp;
    private IndexWriter writer;

    /**
     * @see DirectoryTaxonomyWriter#DirectoryTaxonomyWriter(Directory, IndexWriterConfig.OpenMode,
     *     TaxonomyWriterCache)
     */
    public SnapshotDirectoryTaxonomyWriter(
        Directory directory, OpenMode openMode, TaxonomyWriterCache cache) throws IOException {
      super(directory, openMode, cache);
    }

    /**
     * @see DirectoryTaxonomyWriter#DirectoryTaxonomyWriter(Directory, IndexWriterConfig.OpenMode)
     */
    public SnapshotDirectoryTaxonomyWriter(Directory directory, OpenMode openMode)
        throws IOException {
      super(directory, openMode);
    }

    /** @see DirectoryTaxonomyWriter#DirectoryTaxonomyWriter(Directory) */
    public SnapshotDirectoryTaxonomyWriter(Directory d) throws IOException {
      super(d);
    }

    @Override
    protected IndexWriterConfig createIndexWriterConfig(OpenMode openMode) {
      IndexWriterConfig conf = super.createIndexWriterConfig(openMode);
      sdp = new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy());
      conf.setIndexDeletionPolicy(sdp);
      return conf;
    }

    @Override
    protected IndexWriter openIndexWriter(Directory directory, IndexWriterConfig config)
        throws IOException {
      writer = super.openIndexWriter(directory, config);
      return writer;
    }

    /** Returns the {@link SnapshotDeletionPolicy} used by the underlying {@link IndexWriter}. */
    public SnapshotDeletionPolicy getDeletionPolicy() {
      return sdp;
    }

    /** Returns the {@link IndexWriter} used by this {@link DirectoryTaxonomyWriter}. */
    public IndexWriter getIndexWriter() {
      return writer;
    }
  }

  private static final int RADIX = 16;

  public static final String INDEX_SOURCE = "index";
  public static final String TAXONOMY_SOURCE = "taxo";

  private final IndexWriter indexWriter;
  private final SnapshotDirectoryTaxonomyWriter taxoWriter;
  private final IndexCommit indexCommit, taxoCommit;
  private final SnapshotDeletionPolicy indexSDP, taxoSDP;
  private final String version;
  private final Map<String, List<RevisionFile>> sourceFiles;

  /** Returns a singleton map of the revision files from the given {@link IndexCommit}. */
  public static Map<String, List<RevisionFile>> revisionFiles(
      IndexCommit indexCommit, IndexCommit taxoCommit) throws IOException {
    HashMap<String, List<RevisionFile>> files = new HashMap<>();
    files.put(INDEX_SOURCE, IndexRevision.revisionFiles(indexCommit).values().iterator().next());
    files.put(TAXONOMY_SOURCE, IndexRevision.revisionFiles(taxoCommit).values().iterator().next());
    return files;
  }

  /**
   * Returns a String representation of a revision's version from the given {@link IndexCommit}s of
   * the search and taxonomy indexes.
   */
  public static String revisionVersion(IndexCommit indexCommit, IndexCommit taxoCommit) {
    return Long.toString(indexCommit.getGeneration(), RADIX)
        + ":"
        + Long.toString(taxoCommit.getGeneration(), RADIX);
  }

  /**
   * Constructor over the given {@link IndexWriter}. Uses the last {@link IndexCommit} found in the
   * {@link Directory} managed by the given writer.
   */
  public IndexAndTaxonomyRevision(
      IndexWriter indexWriter, SnapshotDirectoryTaxonomyWriter taxoWriter) throws IOException {
    IndexDeletionPolicy delPolicy = indexWriter.getConfig().getIndexDeletionPolicy();
    if (!(delPolicy instanceof SnapshotDeletionPolicy)) {
      throw new IllegalArgumentException("IndexWriter must be created with SnapshotDeletionPolicy");
    }
    this.indexWriter = indexWriter;
    this.taxoWriter = taxoWriter;
    this.indexSDP = (SnapshotDeletionPolicy) delPolicy;
    this.taxoSDP = taxoWriter.getDeletionPolicy();
    this.indexCommit = indexSDP.snapshot();
    this.taxoCommit = taxoSDP.snapshot();
    this.version = revisionVersion(indexCommit, taxoCommit);
    this.sourceFiles = revisionFiles(indexCommit, taxoCommit);
  }

  @Override
  public int compareTo(String version) {
    final String[] parts = version.split(":");
    final long indexGen = Long.parseLong(parts[0], RADIX);
    final long taxoGen = Long.parseLong(parts[1], RADIX);
    final long indexCommitGen = indexCommit.getGeneration();
    final long taxoCommitGen = taxoCommit.getGeneration();

    // if the index generation is not the same as this commit's generation,
    // compare by it. Otherwise, compare by the taxonomy generation.
    if (indexCommitGen < indexGen) {
      return -1;
    } else if (indexCommitGen > indexGen) {
      return 1;
    } else {
      return taxoCommitGen < taxoGen ? -1 : (taxoCommitGen > taxoGen ? 1 : 0);
    }
  }

  @Override
  public int compareTo(Revision o) {
    IndexAndTaxonomyRevision other = (IndexAndTaxonomyRevision) o;
    int cmp = indexCommit.compareTo(other.indexCommit);
    return cmp != 0 ? cmp : taxoCommit.compareTo(other.taxoCommit);
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public Map<String, List<RevisionFile>> getSourceFiles() {
    return sourceFiles;
  }

  @Override
  public InputStream open(String source, String fileName) throws IOException {
    assert source.equals(INDEX_SOURCE) || source.equals(TAXONOMY_SOURCE)
        : "invalid source; expected=("
            + INDEX_SOURCE
            + " or "
            + TAXONOMY_SOURCE
            + ") got="
            + source;
    IndexCommit ic = source.equals(INDEX_SOURCE) ? indexCommit : taxoCommit;
    return new IndexInputInputStream(ic.getDirectory().openInput(fileName, IOContext.READONCE));
  }

  @Override
  public void release() throws IOException {
    try {
      indexSDP.release(indexCommit);
    } finally {
      taxoSDP.release(taxoCommit);
    }

    try {
      indexWriter.deleteUnusedFiles();
    } finally {
      taxoWriter.getIndexWriter().deleteUnusedFiles();
    }
  }

  @Override
  public String toString() {
    return "IndexAndTaxonomyRevision version=" + version + " files=" + sourceFiles;
  }
}
