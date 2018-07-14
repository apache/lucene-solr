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

package org.apache.lucene.luke.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utilities for various raw index operations.
 *
 * <p>
 * This is for internal uses, DO NOT call from UI components or applications.
 * </p>
 */
public final class IndexUtils {

  /**
   * Opens index(es) reader for given index path.
   *
   * @param indexPath - path to the index directory
   * @param dirImpl - class name for the specific directory implementation
   * @return index reader
   */
  public static IndexReader openIndex(@Nonnull String indexPath, @Nullable String dirImpl)
      throws Exception {
    final Path root = FileSystems.getDefault().getPath(indexPath);
    final List<DirectoryReader> readers = new ArrayList<>();

    // find all valid index directories in this directory
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attrs) throws IOException {
        Directory dir = openDirectory(path, dirImpl);
        try {
          DirectoryReader dr = DirectoryReader.open(dir);
          readers.add(dr);
        } catch (IOException e) {
          // skip
        }
        return FileVisitResult.CONTINUE;
      }
    });

    if (readers.isEmpty()) {
      throw new RuntimeException("No valid directory at the location: " + indexPath);
    }

    if (readers.size() == 1) {
      return readers.get(0);
    } else {
      return new MultiReader(readers.toArray(new IndexReader[readers.size()]));
    }
  }

  /**
   * Opens an index directory for given index path.
   *
   * <p>This can be used to open/repair corrupted indexes.</p>
   *
   * @param dirPath - index directory path
   * @param dirImpl - class name for the specific directory implementation
   * @return directory
   */
  public static Directory openDirectory(@Nonnull String dirPath, @Nullable String dirImpl) throws IOException {
    final Path path = FileSystems.getDefault().getPath(dirPath);
    Directory dir = openDirectory(path, dirImpl);
    return dir;
  }

  private static Directory openDirectory(@Nonnull Path path, String dirImpl) throws IOException {
    if (!Files.exists(path)) {
      throw new IllegalArgumentException("Index directory doesn't exist.");
    }

    Directory dir;
    if (dirImpl == null || dirImpl.equalsIgnoreCase("org.apache.lucene.store.FSDirectory")) {
      dir = FSDirectory.open(path);
    } else {
      try {
        Class<?> implClazz = Class.forName(dirImpl);
        Constructor<?> constr = implClazz.getConstructor(Path.class);
        if (constr != null) {
          dir = (Directory) constr.newInstance(path);
        } else {
          constr = implClazz.getConstructor(Path.class, LockFactory.class);
          dir = (Directory) constr.newInstance(path, null);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid directory implementation class: " + dirImpl);
      }
    }
    return dir;
  }

  /**
   * Close index directory.
   *
   * @param dir - index directory to be closed
   */
  public static void close(Directory dir) {
    try {
      if (dir != null) {
        dir.close();
      }
    } catch (IOException e) {
      throw new LukeException(e.getMessage(), e);
    }
  }

  /**
   * Close index reader.
   *
   * @param reader - index reader to be closed
   */
  public static void close(IndexReader reader) {
    try {
      if (reader != null) {
        reader.close();
        if (reader instanceof DirectoryReader) {
          Directory dir = ((DirectoryReader) reader).directory();
          dir.close();
        }
      }
    } catch (IOException e) {
      throw new LukeException(e.getMessage(), e);
    }
  }

  /**
   * Create an index writer.
   *
   * @param dir - index directory
   * @param analyzer - analyzer used by the index writer
   * @param useCompound - if true, compound index files are used
   * @param keepAllCommits - if true, all commit generations are kept
   * @return new index writer
   */
  public static IndexWriter createWriter(@Nonnull Directory dir, Analyzer analyzer, boolean useCompound, boolean keepAllCommits) throws IOException {
    return createWriter(dir, analyzer, useCompound, keepAllCommits, null);
  }

  /**
   * Create an index writer.
   *
   * @param dir - index directory
   * @param analyzer - analyser used by the index writer
   * @param useCompound - if true, compound index files are used
   * @param keepAllCommits - if true, all commit generations are kept
   * @param ps - information stream
   * @return new index writer
   */
  public static IndexWriter createWriter(@Nonnull Directory dir, Analyzer analyzer, boolean useCompound, boolean keepAllCommits,
                                         @Nullable PrintStream ps) throws IOException {

    IndexWriterConfig config = new IndexWriterConfig(analyzer == null ? new WhitespaceAnalyzer() : analyzer);
    config.setUseCompoundFile(useCompound);
    if (ps != null) {
      config.setInfoStream(ps);
    }
    if (keepAllCommits) {
      config.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    } else {
      config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    }

    return new IndexWriter(dir, config);
  }

  /**
   * Execute force merge with the index writer.
   *
   * @param writer - index writer
   * @param expunge - if true, only segments having deleted documents are merged
   * @param maxNumSegments - max number of segments
   */
  public static void optimizeIndex(@Nonnull IndexWriter writer, boolean expunge, int maxNumSegments) throws IOException {
    if (expunge) {
      writer.forceMergeDeletes(true);
    } else {
      writer.forceMerge(maxNumSegments, true);
    }
  }

  /**
   * Check the index status.
   *
   * @param dir - index directory for checking
   * @param ps - information stream
   * @return - index status
   */
  public static CheckIndex.Status checkIndex(@Nonnull Directory dir, @Nullable PrintStream ps) throws IOException {
    try (CheckIndex ci = new CheckIndex(dir)) {
      if (ps != null) {
        ci.setInfoStream(ps);
      }
      return ci.checkIndex();
    }
  }

  /**
   * Try to repair the corrupted index using previously returned index status.
   *
   * @param dir - index directory for repairing
   * @param st - index status
   * @param ps - information stream
   */
  public static void tryRepairIndex(@Nonnull Directory dir, @Nonnull CheckIndex.Status st, @Nullable PrintStream ps) throws IOException {
    try (CheckIndex ci = new CheckIndex(dir)) {
      if (ps != null) {
        ci.setInfoStream(ps);
      }
      ci.exorciseIndex(st);
    }
  }

  /**
   * Returns the string representation for Lucene codec version when the index was written.
   *
   * @param dir - index directory
   */
  public static String getIndexFormat(@Nonnull Directory dir) throws IOException {
    return new SegmentInfos.FindSegmentsFile<String>(dir) {
      @Override
      protected String doBody(String segmentFileName) throws IOException {
        String format = "unknown";
        try (IndexInput in = dir.openInput(segmentFileName, IOContext.READ)) {
          if (CodecUtil.CODEC_MAGIC == in.readInt()) {
            int actualVersion = CodecUtil.checkHeaderNoMagic(in, "segments", SegmentInfos.VERSION_53, Integer.MAX_VALUE);
            if (actualVersion == SegmentInfos.VERSION_53) {
              format = "Lucene 5.3 or later";
            } else if (actualVersion == SegmentInfos.VERSION_70) {
              format = "Lucene 7.0 or later";
            } else if (actualVersion == SegmentInfos.VERSION_72) {
              format = "Lucene 7.2 or later";
            } else if (actualVersion == SegmentInfos.VERSION_74) {
              format = "Lucene 7.4 or later";
            } else if (actualVersion > SegmentInfos.VERSION_74) {
              format = "Lucene 7.4 or later (UNSUPPORTED)";
            }
          } else {
            format = "Lucene 5.x or prior (UNSUPPORTED)";
          }
        }
        return format;
      }
    }.run();
  }

  /**
   * Returns user data written with the specified commit.
   *
   * @param ic - index commit
   */
  public static String getCommitUserData(@Nonnull IndexCommit ic) throws IOException {
    Map<String, String> userDataMap = ic.getUserData();
    if (userDataMap != null) {
      return userDataMap.toString();
    } else {
      return "--";
    }
  }

  /**
   * Collect all terms and their counts in the specified fields.
   *
   * @param reader - index reader
   * @param fields - field names
   * @return a map contains terms and their occurrence frequencies
   */
  public static Map<String, Long> countTerms(IndexReader reader, Collection<String> fields) throws IOException {
    Map<String, Long> res = new HashMap<>();
    for (String field : fields) {
      if (!res.containsKey(field)) {
        res.put(field, 0L);
      }
      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        TermsEnum te = terms.iterator();
        while (te.next() != null) {
          res.put(field, res.get(field) + 1);
        }
      }
    }
    return res;
  }

  /**
   * Returns the {@link Bits} representing live documents in the index.
   *
   * @param reader - index reader
   */
  public static Bits getLiveDocs(IndexReader reader) {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getLiveDocs();
    } else {
      return MultiFields.getLiveDocs(reader);
    }
  }

  /**
   * Returns field {@link FieldInfos} in the index.
   *
   * @param reader - index reader
   */
  public static FieldInfos getFieldInfos(IndexReader reader) {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getFieldInfos();
    } else {
      return MultiFields.getMergedFieldInfos(reader);
    }
  }

  /**
   * Returns the {@link FieldInfo} referenced by the field.
   *
   * @param reader - index reader
   * @param fieldName - field name
   */
  public static FieldInfo getFieldInfo(IndexReader reader, String fieldName) {
    return getFieldInfos(reader).fieldInfo(fieldName);
  }

  /**
   * Returns all field names in the index.
   *
   * @param reader - index reader
   */
  public static Collection<String> getFieldNames(IndexReader reader) {
    return StreamSupport.stream(getFieldInfos(reader).spliterator(), false)
        .map(f -> f.name)
        .collect(Collectors.toList());
  }

  /**
   * Returns the {@link Terms} for the specified field.
   *
   * @param reader - index reader
   * @param field - field name
   */
  public static Terms getTerms(IndexReader reader, String field) throws IOException {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).terms(field);
    } else {
      return MultiFields.getTerms(reader, field);
    }
  }

  /**
   * Returns the {@link BinaryDocValues} for the specified field.
   *
   * @param reader - index reader
   * @param field - field name
   */
  public static BinaryDocValues getBinaryDocValues(IndexReader reader, String field) throws IOException {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getBinaryDocValues(field);
    } else {
      return MultiDocValues.getBinaryValues(reader, field);
    }
  }

  /**
   * Returns the {@link NumericDocValues} for the specified field.
   *
   * @param reader - index reader
   * @param field - field name
   */
  public static NumericDocValues getNumericDocValues(IndexReader reader, String field) throws IOException {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getNumericDocValues(field);
    } else {
      return MultiDocValues.getNumericValues(reader, field);
    }
  }

  /**
   * Returns the {@link SortedNumericDocValues} for the specified field.
   *
   * @param reader - index reader
   * @param field - field name
   */
  public static SortedNumericDocValues getSortedNumericDocValues(IndexReader reader, String field) throws IOException {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getSortedNumericDocValues(field);
    } else {
      return MultiDocValues.getSortedNumericValues(reader, field);
    }
  }

  /**
   * Returns the {@link SortedDocValues} for the specified field.
   *
   * @param reader - index reader
   * @param field - field name
   */
  public static SortedDocValues getSortedDocValues(IndexReader reader, String field) throws IOException {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getSortedDocValues(field);
    } else {
      return MultiDocValues.getSortedValues(reader, field);
    }
  }

  /**
   * Returns the {@link SortedSetDocValues} for the specified field.
   *
   * @param reader - index reader
   * @param field - field name
   */
  public static SortedSetDocValues getSortedSetDocvalues(IndexReader reader, String field) throws IOException {
    if (reader instanceof LeafReader) {
      return ((LeafReader) reader).getSortedSetDocValues(field);
    } else {
      return MultiDocValues.getSortedSetValues(reader, field);
    }
  }

  private IndexUtils() {
  }
}
