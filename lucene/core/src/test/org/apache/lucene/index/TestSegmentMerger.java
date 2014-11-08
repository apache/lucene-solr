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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

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
    SegmentCommitInfo info1 = DocHelper.writeDoc(random(), merge1Dir);
    SegmentCommitInfo info2 = DocHelper.writeDoc(random(), merge2Dir);
    reader1 = new SegmentReader(FieldTypes.getFieldTypes(merge1Dir, null), info1, newIOContext(random()));
    reader2 = new SegmentReader(FieldTypes.getFieldTypes(merge2Dir, null), info2, newIOContext(random()));
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
    final SegmentInfo si = new SegmentInfo(mergedDir, Version.LATEST, mergedSegment, -1, false, codec, null, StringHelper.randomId());

    FieldTypes fieldTypes = FieldTypes.getFieldTypes(merge1Dir, new MockAnalyzer(random()));
    SegmentMerger merger = new SegmentMerger(fieldTypes, Arrays.<LeafReader>asList(reader1, reader2),
        si, InfoStream.getDefault(), mergedDir,
        MergeState.CheckAbort.NONE, new FieldInfos.FieldNumbers(), newIOContext(random()));
    MergeState mergeState = merger.merge();
    int docsMerged = mergeState.segmentInfo.getDocCount();
    assertTrue(docsMerged == 2);
    //Should be able to open a new SegmentReader against the new directory
    SegmentReader mergedReader = new SegmentReader(fieldTypes,
                                                   new SegmentCommitInfo(
                                                         mergeState.segmentInfo,
                                                         0, -1L, -1L, -1L),
                                                   newIOContext(random()));
    assertTrue(mergedReader != null);
    assertTrue(mergedReader.numDocs() == 2);
    Document2 newDoc1 = mergedReader.document(0);
    assertTrue(newDoc1 != null);

    Set<String> unstored = DocHelper.getUnstored(fieldTypes);

    //There are 2 unstored fields on the document
    assertEquals(DocHelper.numFields() - unstored.size() + 1, DocHelper.numFields(newDoc1));
    Document2 newDoc2 = mergedReader.document(1);
    assertTrue(newDoc2 != null);
    assertEquals(DocHelper.numFields() - unstored.size() + 1, DocHelper.numFields(newDoc2));

    DocsEnum termDocs = TestUtil.docs(random(), mergedReader,
        DocHelper.TEXT_FIELD_2_KEY,
        new BytesRef("field"),
        MultiFields.getLiveDocs(mergedReader),
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
    TermsEnum termsEnum = vector.iterator(null);

    int i = 0;
    while (termsEnum.next() != null) {
      String term = termsEnum.term().utf8ToString();
      int freq = (int) termsEnum.totalTermFreq();
      //System.out.println("Term: " + term + " Freq: " + freq);
      assertTrue(DocHelper.FIELD_2_TEXT.indexOf(term) != -1);
      assertTrue(DocHelper.FIELD_2_FREQS[i] == freq);
      i++;
    }

    TestSegmentReader.checkNorms(fieldTypes, mergedReader);
    mergedReader.close();
  }

  private static boolean equals(MergeState.DocMap map1, MergeState.DocMap map2) {
    if (map1.maxDoc() != map2.maxDoc()) {
      return false;
    }
    for (int i = 0; i < map1.maxDoc(); ++i) {
      if (map1.get(i) != map2.get(i)) {
        return false;
      }
    }
    return true;
  }

  public void testBuildDocMap() {
    final int maxDoc = TestUtil.nextInt(random(), 1, 128);
    final int numDocs = TestUtil.nextInt(random(), 0, maxDoc);
    final int numDeletedDocs = maxDoc - numDocs;
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

    final MergeState.DocMap docMap = MergeState.DocMap.build(maxDoc, liveDocs);

    assertEquals(maxDoc, docMap.maxDoc());
    assertEquals(numDocs, docMap.numDocs());
    assertEquals(numDeletedDocs, docMap.numDeletedDocs());
    // assert the mapping is compact
    for (int i = 0, del = 0; i < maxDoc; ++i) {
      if (!liveDocs.get(i)) {
        assertEquals(-1, docMap.get(i));
        ++del;
      } else {
        assertEquals(i - del, docMap.get(i));
      }
    }
  }
}
