package org.apache.lucene.index;

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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.DoubleBarrelLRUCache;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RamUsageTester;

/**
 * Common tests to all index formats.
 */
abstract class BaseIndexFileFormatTestCase extends LuceneTestCase {

  // metadata or Directory-level objects
  private static final Set<Class<?>> EXCLUDED_CLASSES = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());

  static {
    // Directory objects, don't take into account eg. the NIO buffers
    EXCLUDED_CLASSES.add(Directory.class);
    EXCLUDED_CLASSES.add(IndexInput.class);
    EXCLUDED_CLASSES.add(IntIndexInput.class);

    // used for thread management, not by the index
    EXCLUDED_CLASSES.add(CloseableThreadLocal.class);
    EXCLUDED_CLASSES.add(ThreadLocal.class);

    // don't follow references to the top-level reader
    EXCLUDED_CLASSES.add(IndexReader.class);
    EXCLUDED_CLASSES.add(IndexReaderContext.class);

    // usually small but can bump memory usage for
    // memory-efficient things like stored fields
    EXCLUDED_CLASSES.add(FieldInfos.class);
    EXCLUDED_CLASSES.add(SegmentInfo.class);
    EXCLUDED_CLASSES.add(SegmentCommitInfo.class);
    EXCLUDED_CLASSES.add(FieldInfo.class);

    // used by lucene3x to maintain a cache. Doesn't depend on the number of docs
    EXCLUDED_CLASSES.add(DoubleBarrelLRUCache.class);

    // constant overhead is typically due to strings
    // TODO: can we remove this and still pass the test consistently
    EXCLUDED_CLASSES.add(String.class);
  }

  static class Accumulator extends RamUsageTester.Accumulator {

    private final Object root;

    Accumulator(Object root) {
      this.root = root;
    }

    public long accumulateObject(Object o, long shallowSize, java.util.Map<Field, Object> fieldValues, java.util.Collection<Object> queue) {
      for (Class<?> clazz = o.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
        if (EXCLUDED_CLASSES.contains(clazz) && o != root) {
          return 0;
        }
      }
      // we have no way to estimate the size of these things in codecs although
      // something like a Collections.newSetFromMap(new HashMap<>()) uses quite
      // some memory... So for now the test ignores the overhead of such
      // collections but can we do better?
      if (o instanceof Collection) {
        Collection<?> coll = (Collection<?>) o;
        queue.addAll((Collection<?>) o);
        return (long) coll.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      } else if (o instanceof Map) {
        final Map<?, ?> map = (Map<?,?>) o;
        queue.addAll(map.keySet());
        queue.addAll(map.values());
        return 2L * map.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      }
      long v = super.accumulateObject(o, shallowSize, fieldValues, queue);
      // System.out.println(o.getClass() + "=" + v);
      return v;
    }

    @Override
    public long accumulateArray(Object array, long shallowSize,
        List<Object> values, Collection<Object> queue) {
      long v = super.accumulateArray(array, shallowSize, values, queue);
      // System.out.println(array.getClass() + "=" + v);
      return v;
    }

  };

  /** Returns the codec to run tests against */
  protected abstract Codec getCodec();

  private Codec savedCodec;

  public void setUp() throws Exception {
    super.setUp();
    // set the default codec, so adding test cases to this isn't fragile
    savedCodec = Codec.getDefault();
    Codec.setDefault(getCodec());
  }

  public void tearDown() throws Exception {
    Codec.setDefault(savedCodec); // restore
    super.tearDown();
  }

  /** Add random fields to the provided document. */
  protected abstract void addRandomFields(Document doc);

  private Map<String, Long> bytesUsedByExtension(Directory d) throws IOException {
    Map<String, Long> bytesUsedByExtension = new HashMap<>();
    for (String file : d.listAll()) {
      final String ext = IndexFileNames.getExtension(file);
      final long previousLength = bytesUsedByExtension.containsKey(ext) ? bytesUsedByExtension.get(ext) : 0;
      bytesUsedByExtension.put(ext, previousLength + d.fileLength(file));
    }
    bytesUsedByExtension.keySet().removeAll(excludedExtensionsFromByteCounts());

    return bytesUsedByExtension;
  }

  /**
   * Return the list of extensions that should be excluded from byte counts when
   * comparing indices that store the same content.
   */
  protected Collection<String> excludedExtensionsFromByteCounts() {
    return new HashSet<String>(Arrays.asList(new String[] {
    // segment infos store various pieces of information that don't solely depend
    // on the content of the index in the diagnostics (such as a timestamp) so we
    // exclude this file from the bytes counts
                        "si",
    // lock files are 0 bytes (one directory in the test could be RAMDir, the other FSDir)
                        "lock" }));
  }

  /** The purpose of this test is to make sure that bulk merge doesn't accumulate useless data over runs. */
  public void testMergeStability() throws Exception {
    Directory dir = newDirectory();
    // do not use newMergePolicy that might return a MockMergePolicy that ignores the no-CFS ratio
    // do not use RIW which will change things up!
    MergePolicy mp = newTieredMergePolicy();
    mp.setNoCFSRatio(0);
    IndexWriterConfig cfg = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setUseCompoundFile(false).setMergePolicy(mp);
    IndexWriter w = new IndexWriter(dir, cfg);
    final int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; ++i) {
      Document d = new Document();
      addRandomFields(d);
      w.addDocument(d);
    }
    w.forceMerge(1);
    w.commit();
    w.close();
    IndexReader reader = DirectoryReader.open(dir);

    Directory dir2 = newDirectory();
    mp = newTieredMergePolicy();
    mp.setNoCFSRatio(0);
    cfg = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setUseCompoundFile(false).setMergePolicy(mp);
    w = new IndexWriter(dir2, cfg);
    w.addIndexes(reader);
    w.commit();
    w.close();

    assertEquals(bytesUsedByExtension(dir), bytesUsedByExtension(dir2));

    reader.close();
    dir.close();
    dir2.close();
  }

  /** Test the accuracy of the ramBytesUsed estimations. */
  public void testRamBytesUsed() throws IOException {
    if (Codec.getDefault() instanceof RandomCodec) {
      // this test relies on the fact that two segments will be written with
      // the same codec so we need to disable MockRandomPF
      final Set<String> avoidCodecs = new HashSet<>(((RandomCodec) Codec.getDefault()).avoidCodecs);
      avoidCodecs.add(new MockRandomPostingsFormat().getName());
      Codec.setDefault(new RandomCodec(random(), avoidCodecs));
    }
    Directory dir = newDirectory();
    IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, cfg);
    // we need to index enough documents so that constant overhead doesn't dominate
    final int numDocs = atLeast(10000);
    AtomicReader reader1 = null;
    for (int i = 0; i < numDocs; ++i) {
      Document d = new Document();
      addRandomFields(d);
      w.addDocument(d);
      if (i == 100) {
        w.forceMerge(1);
        w.commit();
        reader1 = getOnlySegmentReader(DirectoryReader.open(dir));
      }
    }
    w.forceMerge(1);
    w.commit();
    w.close();

    AtomicReader reader2 = getOnlySegmentReader(DirectoryReader.open(dir));

    for (AtomicReader reader : Arrays.asList(reader1, reader2)) {
      new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT).warm(reader);
    }

    final long actualBytes = RamUsageTester.sizeOf(reader2, new Accumulator(reader2)) - RamUsageTester.sizeOf(reader1, new Accumulator(reader1));
    final long expectedBytes = ((SegmentReader) reader2).ramBytesUsed() - ((SegmentReader) reader1).ramBytesUsed();
    final long absoluteError = actualBytes - expectedBytes;
    final double relativeError = (double) absoluteError / actualBytes;
    final String message = "Actual RAM usage " + actualBytes + ", but got " + expectedBytes + ", " + 100*relativeError + "% error";
    assertTrue(message, Math.abs(relativeError) < 0.20d || Math.abs(absoluteError) < 1000);

    reader1.close();
    reader2.close();
    dir.close();
  }

}
