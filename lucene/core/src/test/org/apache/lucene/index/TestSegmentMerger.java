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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.packed.PackedLongValues;

public class TestSegmentMerger extends LuceneTestCase {
  //The variables for the new merged segment
  private Directory mergedDir;
  private String mergedSegment = "test";
  //First segment to be merged
  private Directory merge1Dir;
  private Document doc1 = new Document();
  private SegmentReader reader1 = null;
  //Second Segment to be merged
  private Directory merge2Dir;
  private Document doc2 = new Document();
  private SegmentReader reader2 = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mergedDir = newDirectory();
    merge1Dir = newDirectory();
    merge2Dir = newDirectory();
    DocHelper.setupDoc(doc1);
    SegmentCommitInfo info1 = DocHelper.writeDoc(random(), merge1Dir, doc1);
    DocHelper.setupDoc(doc2);
    SegmentCommitInfo info2 = DocHelper.writeDoc(random(), merge2Dir, doc2);
    reader1 = new SegmentReader(info1, Version.LATEST.major, newIOContext(random()));
    reader2 = new SegmentReader(info2, Version.LATEST.major, newIOContext(random()));
  }

  @Override
  public void tearDown() throws Exception {
    reader1.close();
    reader2.close();
    mergedDir.close();
    merge1Dir.close();
    merge2Dir.close();
    super.tearDown();
  }

  public void test() {
    assertTrue(mergedDir != null);
    assertTrue(merge1Dir != null);
    assertTrue(merge2Dir != null);
    assertTrue(reader1 != null);
    assertTrue(reader2 != null);
  }

  public void testMerge() throws IOException {
    final Codec codec = Codec.getDefault();
    final SegmentInfo si = new SegmentInfo(mergedDir, Version.LATEST, null, mergedSegment, -1, false, codec, Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);

    SegmentMerger merger = new SegmentMerger(Arrays.<CodecReader>asList(reader1, reader2),
                                             si, InfoStream.getDefault(), mergedDir,
                                             new FieldInfos.FieldNumbers(null),
                                             newIOContext(random(), new IOContext(new MergeInfo(-1, -1, false, -1))));
    MergeState mergeState = merger.merge();
    int docsMerged = mergeState.segmentInfo.maxDoc();
    assertTrue(docsMerged == 2);
    //Should be able to open a new SegmentReader against the new directory
    SegmentReader mergedReader = new SegmentReader(new SegmentCommitInfo(
        mergeState.segmentInfo,
        0, 0, -1L, -1L, -1L, StringHelper.randomId()),
        Version.LATEST.major,
        newIOContext(random()));
    assertTrue(mergedReader != null);
    assertTrue(mergedReader.numDocs() == 2);
    Document newDoc1 = mergedReader.document(0);
    assertTrue(newDoc1 != null);
    //There are 2 unstored fields on the document
    assertTrue(DocHelper.numFields(newDoc1) == DocHelper.numFields(doc1) - DocHelper.unstored.size());
    Document newDoc2 = mergedReader.document(1);
    assertTrue(newDoc2 != null);
    assertTrue(DocHelper.numFields(newDoc2) == DocHelper.numFields(doc2) - DocHelper.unstored.size());

    PostingsEnum termDocs = TestUtil.docs(random(), mergedReader,
        DocHelper.TEXT_FIELD_2_KEY,
        new BytesRef("field"),
        null,
        0);
    assertTrue(termDocs != null);
    assertTrue(termDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    int tvCount = 0;
    for(FieldInfo fieldInfo : mergedReader.getFieldInfos()) {
      if (fieldInfo.hasVectors()) {
        tvCount++;
      }
    }
    
    //System.out.println("stored size: " + stored.size());
    assertEquals("We do not have 3 fields that were indexed with term vector", 3, tvCount);

    Terms vector = mergedReader.getTermVectors(0).terms(DocHelper.TEXT_FIELD_2_KEY);
    assertNotNull(vector);
    assertEquals(3, vector.size());
    TermsEnum termsEnum = vector.iterator();

    int i = 0;
    while (termsEnum.next() != null) {
      String term = termsEnum.term().utf8ToString();
      int freq = (int) termsEnum.totalTermFreq();
      //System.out.println("Term: " + term + " Freq: " + freq);
      assertTrue(DocHelper.FIELD_2_TEXT.indexOf(term) != -1);
      assertTrue(DocHelper.FIELD_2_FREQS[i] == freq);
      i++;
    }

    TestSegmentReader.checkNorms(mergedReader);
    mergedReader.close();
  }

  public void testBuildDocMap() {
    final int maxDoc = TestUtil.nextInt(random(), 1, 128);
    final int numDocs = TestUtil.nextInt(random(), 0, maxDoc);
    final FixedBitSet liveDocs = new FixedBitSet(maxDoc);
    for (int i = 0; i < numDocs; ++i) {
      while (true) {
        final int docID = random().nextInt(maxDoc);
        if (!liveDocs.get(docID)) {
          liveDocs.set(docID);
          break;
        }
      }
    }

    final PackedLongValues docMap = MergeState.removeDeletes(maxDoc, liveDocs);

    // assert the mapping is compact
    for (int i = 0, del = 0; i < maxDoc; ++i) {
      if (liveDocs.get(i) == false) {
        ++del;
      } else {
        assertEquals(i - del, docMap.get(i));
      }
    }
  }
}
