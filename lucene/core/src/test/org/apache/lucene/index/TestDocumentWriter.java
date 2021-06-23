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
import java.util.function.Function;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestDocumentWriter extends LuceneTestCase {
  private Directory dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  public void test() {
    assertTrue(dir != null);
  }

  public void testAddDocument() throws Exception {
    Document testDoc = new Document();
    DocHelper.setupDoc(testDoc);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.addDocument(testDoc);
    writer.commit();
    SegmentCommitInfo info = writer.newestSegment();
    writer.close();
    //After adding the document, we should be able to read it back in
    SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));
    assertTrue(reader != null);
    Document doc = reader.document(0);
    assertTrue(doc != null);

    //System.out.println("Document: " + doc);
    IndexableField[] fields = doc.getFields("textField2");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_2_TEXT));
    assertTrue(fields[0].fieldType().storeTermVectors());

    fields = doc.getFields("textField1");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_1_TEXT));
    assertFalse(fields[0].fieldType().storeTermVectors());

    fields = doc.getFields("keyField");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.KEYWORD_TEXT));

    fields = doc.getFields(DocHelper.NO_NORMS_KEY);
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.NO_NORMS_TEXT));

    fields = doc.getFields(DocHelper.TEXT_FIELD_3_KEY);
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_3_TEXT));

    // test that the norms are not present in the segment if
    // omitNorms is true
    for (FieldInfo fi : reader.getFieldInfos()) {
      if (fi.getIndexOptions() != IndexOptions.NONE) {
        assertTrue(fi.omitsNorms() == (reader.getNormValues(fi.name) == null));
      }
    }
    reader.close();
  }

  public void testPositionIncrementGap() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        return 500;
      }
    };

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer));

    Document doc = new Document();
    doc.add(newTextField("repeated", "repeated one", Field.Store.YES));
    doc.add(newTextField("repeated", "repeated two", Field.Store.YES));

    writer.addDocument(doc);
    writer.commit();
    SegmentCommitInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));

    PostingsEnum termPositions = MultiTerms.getTermPostingsEnum(reader, "repeated", new BytesRef("repeated"));
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    int freq = termPositions.freq();
    assertEquals(2, freq);
    assertEquals(0, termPositions.nextPosition());
    assertEquals(502, termPositions.nextPosition());
    reader.close();
  }

  public void testTokenReuse() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new TokenFilter(tokenizer) {
          boolean first = true;
          AttributeSource.State state;

          @Override
          public boolean incrementToken() throws IOException {
            if (state != null) {
              restoreState(state);
              payloadAtt.setPayload(null);
              posIncrAtt.setPositionIncrement(0);
              termAtt.setEmpty().append("b");
              state = null;
              return true;
            }

            boolean hasNext = input.incrementToken();
            if (!hasNext) return false;
            if (Character.isDigit(termAtt.buffer()[0])) {
              posIncrAtt.setPositionIncrement(termAtt.buffer()[0] - '0');
            }
            if (first) {
              // set payload on first position only
              payloadAtt.setPayload(new BytesRef(new byte[]{100}));
              first = false;
            }

            // index a "synonym" for every token
            state = captureState();
            return true;

          }

          @Override
          public void reset() throws IOException {
            super.reset();
            first = true;
            state = null;
          }

          final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
          final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
          final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
        });
      }
    };

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer));

    Document doc = new Document();
    doc.add(newTextField("f1", "a 5 a a", Field.Store.YES));

    writer.addDocument(doc);
    writer.commit();
    SegmentCommitInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));

    PostingsEnum termPositions = MultiTerms.getTermPostingsEnum(reader, "f1", new BytesRef("a"));
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    int freq = termPositions.freq();
    assertEquals(3, freq);
    assertEquals(0, termPositions.nextPosition());
    assertNotNull(termPositions.getPayload());
    assertEquals(6, termPositions.nextPosition());
    assertNull(termPositions.getPayload());
    assertEquals(7, termPositions.nextPosition());
    assertNull(termPositions.getPayload());
    reader.close();
  }


  public void testPreAnalyzedField() throws IOException {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();

    doc.add(new TextField("preanalyzed", new TokenStream() {
      private String[] tokens = new String[] {"term1", "term2", "term3", "term2"};
      private int index = 0;
      
      private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      
      @Override
      public boolean incrementToken() {
        if (index == tokens.length) {
          return false;
        } else {
          clearAttributes();
          termAtt.setEmpty().append(tokens[index++]);
          return true;
        }        
      }
      }));
    
    writer.addDocument(doc);
    writer.commit();
    SegmentCommitInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = new SegmentReader(info, Version.LATEST.major, newIOContext(random()));

    PostingsEnum termPositions = reader.postings(new Term("preanalyzed", "term1"), PostingsEnum.ALL);
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, termPositions.freq());
    assertEquals(0, termPositions.nextPosition());

    termPositions = reader.postings(new Term("preanalyzed", "term2"), PostingsEnum.ALL);
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(2, termPositions.freq());
    assertEquals(1, termPositions.nextPosition());
    assertEquals(3, termPositions.nextPosition());
    
    termPositions = reader.postings(new Term("preanalyzed", "term3"), PostingsEnum.ALL);
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, termPositions.freq());
    assertEquals(2, termPositions.nextPosition());
    reader.close();
  }

  /**
   * Test adding two fields with the same name, one indexed
   * the other stored only. The omitNorms and omitTermFreqAndPositions setting
   * of the stored field should not affect the indexed one (LUCENE-1590)
   */
  public void testLUCENE_1590() throws Exception {
    Document doc = new Document();
    // f1 has no norms
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    FieldType customType2 = new FieldType();
    customType2.setStored(true);
    doc.add(newField("f1", "v1", customType));
    doc.add(newField("f1", "v2", customType2));
    // f2 has no TF
    FieldType customType3 = new FieldType(TextField.TYPE_NOT_STORED);
    customType3.setIndexOptions(IndexOptions.DOCS);
    Field f = newField("f2", "v1", customType3);
    doc.add(f);
    doc.add(newField("f2", "v2", customType2));

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.addDocument(doc);
    writer.forceMerge(1); // be sure to have a single segment
    writer.close();

    TestUtil.checkIndex(dir);

    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(dir));
    FieldInfos fi = reader.getFieldInfos();
    // f1
    assertFalse("f1 should have no norms", fi.fieldInfo("f1").hasNorms());
    assertEquals("omitTermFreqAndPositions field bit should not be set for f1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f1").getIndexOptions());
    // f2
    assertTrue("f2 should have norms", fi.fieldInfo("f2").hasNorms());
    assertEquals("omitTermFreqAndPositions field bit should be set for f2", IndexOptions.DOCS, fi.fieldInfo("f2").getIndexOptions());
    reader.close();
  }

  /** Make sure that every new field doesn't increment memory usage by more than 16kB */
  private void doTestRAMUsage(Function<String, IndexableField> fieldSupplier) throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                .setMaxBufferedDocs(10)
                .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH))) {
      Document doc = new Document();
      final int numFields = 100;
      for (int i = 0; i < numFields; ++i) {
        doc.add(fieldSupplier.apply("f" + i));
      }
      w.addDocument(doc);
      assertTrue(w.hasChangesInRam());
      assertTrue(w.ramBytesUsed() < numFields * 16384L);
    }
  }

  public void testRAMUsageStored() throws IOException {
    doTestRAMUsage(field -> new StoredField(field, new BytesRef("Lucene")));
  }

  public void testRAMUsageIndexed() throws IOException {
    doTestRAMUsage(field -> new StringField(field, new BytesRef("Lucene"), Store.NO));
  }

  public void testRAMUsagePoint() throws IOException {
    doTestRAMUsage(field -> new IntPoint(field, 42));
  }

  public void testRAMUsageNumericDocValue() throws IOException {
    doTestRAMUsage(field -> new NumericDocValuesField(field, 42));
  }

  public void testRAMUsageSortedDocValue() throws IOException {
    doTestRAMUsage(field -> new SortedDocValuesField(field, new BytesRef("Lucene")));
  }

  public void testRAMUsageBinaryDocValue() throws IOException {
    doTestRAMUsage(field -> new BinaryDocValuesField(field, new BytesRef("Lucene")));
  }

  public void testRAMUsageSortedNumericDocValue() throws IOException {
    doTestRAMUsage(field -> new SortedNumericDocValuesField(field, 42));
  }

  public void testRAMUsageSortedSetDocValue() throws IOException {
    doTestRAMUsage(field -> new SortedSetDocValuesField(field, new BytesRef("Lucene")));
  }

}