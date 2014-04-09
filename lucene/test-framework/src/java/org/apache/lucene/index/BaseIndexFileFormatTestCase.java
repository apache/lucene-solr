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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Common tests to all index formats.
 */
abstract class BaseIndexFileFormatTestCase extends LuceneTestCase {

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
    // segment infos store various pieces of information that don't solely depend
    // on the content of the index in the diagnostics (such as a timestamp) so we
    // exclude this file from the bytes counts
    return Collections.singleton("si");
  }

  /** The purpose of this test is to make sure that bulk merge doesn't accumulate useless data over runs. */
  public void testMergeStability() throws Exception {
    Directory dir = newDirectory();
    // do not use newMergePolicy that might return a MockMergePolicy that ignores the no-CFS ratio
    MergePolicy mp = newTieredMergePolicy();
    mp.setNoCFSRatio(0);
    IndexWriterConfig cfg = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setUseCompoundFile(false).setMergePolicy(mp);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, cfg);
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
    w = new RandomIndexWriter(random(), dir2, cfg);
    w.addIndexes(reader);
    w.commit();
    w.close();

    assertEquals(bytesUsedByExtension(dir), bytesUsedByExtension(dir2));

    reader.close();
    dir.close();
    dir2.close();
  }

}
