package org.apache.lucene.search.positions;
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
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.nestedpulsing.NestedPulsingPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestPositionsAndOffsets extends LuceneTestCase {

  // What am I testing here?
  // - can get offsets out of a basic TermQuery, and a more complex BooleanQuery
  // - if offsets are not stored, then we get -1 returned

  IndexWriterConfig iwc;

  public void setUp() throws Exception {
    super.setUp();

    // Currently only SimpleText and Lucene40 can index offsets into postings:
    String codecName = Codec.getDefault().getName();
    assumeTrue("Codec does not support offsets: " + codecName,
        codecName.equals("SimpleText") ||
            codecName.equals("Lucene40"));

    iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));

    if (codecName.equals("Lucene40")) {
      // Sep etc are not implemented
      switch(random().nextInt(4)) {
        case 0: iwc.setCodec(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat())); break;
        case 1: iwc.setCodec(_TestUtil.alwaysPostingsFormat(new MemoryPostingsFormat())); break;
        case 2: iwc.setCodec(_TestUtil.alwaysPostingsFormat(
            new Pulsing40PostingsFormat(_TestUtil.nextInt(random(), 1, 3)))); break;
        case 3: iwc.setCodec(_TestUtil.alwaysPostingsFormat(new NestedPulsingPostingsFormat())); break;
      }
    }
  }


  private static void addDocs(RandomIndexWriter writer, boolean withOffsets) throws IOException {
    FieldType fieldType = TextField.TYPE_STORED;
    if (withOffsets) {
      fieldType = new FieldType(fieldType);
      fieldType.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    }
    Document doc = new Document();
    doc.add(newField(
        "field",
        "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
            + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
        fieldType));
    writer.addDocument(doc);
  }

  private void testQuery(Query query, int[][] expectedOffsets) throws IOException {
    testQuery(query, expectedOffsets, true);
  }

  private void testQuery(Query query, int[][] expectedOffsets, boolean needsOffsets) throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, iwc);
    addDocs(writer, needsOffsets);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();

    Weight weight = query.createWeight(searcher);
    IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    List<AtomicReaderContext> leaves = topReaderContext.leaves();
    assertEquals(1, leaves.size());
    Scorer scorer = weight.scorer(leaves.get(0),
        true, true, true, false, false, leaves.get(0).reader().getLiveDocs());

    int nextDoc = scorer.nextDoc();
    assertEquals(0, nextDoc);
    IntervalIterator positions = scorer.positions();
    int startOffsets[] = expectedOffsets[0];
    int endOffsets[] = expectedOffsets[1];

    assertEquals(0, positions.scorerAdvanced(nextDoc));
    for (int i = 0; i < startOffsets.length; i++) {
      Interval interval = positions.next();
      assertEquals("i: " + i, startOffsets[i], interval.offsetBegin);
      assertEquals("i: " + i, endOffsets[i], interval.offsetEnd);
    }

    assertNull(positions.next());

    reader.close();
    directory.close();
  }

  public void testTermQueryWithoutOffsets() throws IOException {
    Query query = new TermQuery(new Term("field", "porridge"));
       int[] startOffsets = new int[] { 6, 26, 47, 164, 184 };
        int[] endOffsets = new int[] { 14, 34, 55, 172, 192 };
        testQuery(query, new int[][] { startOffsets, endOffsets });
  }

  public void testBooleanQueryWithOffsets() throws IOException {
    
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")),
        BooleanClause.Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "nine")),
        BooleanClause.Occur.MUST));
    int[] startOffsetsConj = new int[] {47,143};
    int[] endOffsetsConj = new int[] {71, 172};
    testQuery(query, new int[][] {startOffsetsConj, endOffsetsConj});
  }
   
  public void testExactPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "hot!"));
    int[] startOffsetsBlock = new int[] {0, 158};
    int[] endOffsetsBlock = new int[] {19, 177};
    testQuery(query, new int[][] {startOffsetsBlock, endOffsetsBlock});
  }
  
  public void testSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "hot!"));
    query.setSlop(1);
    int[] startOffsetsBlock = new int[] {0, 158};
    int[] endOffsetsBlock = new int[] {19, 177};
    testQuery(query, new int[][] {startOffsetsBlock, endOffsetsBlock});
  }
  
  public void testManyTermSloppyPhraseQuery() throws IOException {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query.add(new Term("field", "pot"));
    query.setSlop(2);
    int[] startOffsetsBlock = new int[] {41};
    int[] endOffsetsBlock = new int[] {66};
    testQuery(query, new int[][] {startOffsetsBlock, endOffsetsBlock});
  }
  
  public void testMultiTermPhraseQuery() throws IOException {
    MultiPhraseQuery query = new MultiPhraseQuery();
    query.add(new Term("field", "pease"));
    query.add(new Term("field", "porridge"));
    query
        .add(new Term[] {new Term("field", "hot!"), new Term("field", "cold!")});
    int[] startOffsetsBlock = new int[] {0, 20, 158, 178};
    int[] endOffsetsBlock = new int[] {19, 40, 177, 198};
    testQuery(query, new int[][] {startOffsetsBlock, endOffsetsBlock});
  }
}