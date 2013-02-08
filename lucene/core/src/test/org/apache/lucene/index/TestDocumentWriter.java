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
import java.io.Reader;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer.addDocument(testDoc);
    writer.commit();
    SegmentInfoPerCommit info = writer.newestSegment();
    writer.close();
    //After adding the document, we should be able to read it back in
    SegmentReader reader = new SegmentReader(info, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random()));
    assertTrue(reader != null);
    Document doc = reader.document(0);
    assertTrue(doc != null);

    //System.out.println("Document: " + doc);
    IndexableField [] fields = doc.getFields("textField2");
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
      if (fi.isIndexed()) {
        assertTrue(fi.omitsNorms() == (reader.getNormValues(fi.name) == null));
      }
    }
    reader.close();
  }

  public void testPositionIncrementGap() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new MockTokenizer(reader, MockTokenizer.WHITESPACE, false));
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        return 500;
      }
    };

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));

    Document doc = new Document();
    doc.add(newTextField("repeated", "repeated one", Field.Store.YES));
    doc.add(newTextField("repeated", "repeated two", Field.Store.YES));

    writer.addDocument(doc);
    writer.commit();
    SegmentInfoPerCommit info = writer.newestSegment();
    writer.close();
    SegmentReader reader = new SegmentReader(info, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random()));

    DocsAndPositionsEnum termPositions = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader),
                                                                          "repeated", new BytesRef("repeated"));
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
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
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

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));

    Document doc = new Document();
    doc.add(newTextField("f1", "a 5 a a", Field.Store.YES));

    writer.addDocument(doc);
    writer.commit();
    SegmentInfoPerCommit info = writer.newestSegment();
    writer.close();
    SegmentReader reader = new SegmentReader(info, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random()));

    DocsAndPositionsEnum termPositions = MultiFields.getTermPositionsEnum(reader, reader.getLiveDocs(), "f1", new BytesRef("a"));
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
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
    SegmentInfoPerCommit info = writer.newestSegment();
    writer.close();
    SegmentReader reader = new SegmentReader(info, DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random()));

    DocsAndPositionsEnum termPositions = reader.termPositionsEnum(new Term("preanalyzed", "term1"));
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, termPositions.freq());
    assertEquals(0, termPositions.nextPosition());

    termPositions = reader.termPositionsEnum(new Term("preanalyzed", "term2"));
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(2, termPositions.freq());
    assertEquals(1, termPositions.nextPosition());
    assertEquals(3, termPositions.nextPosition());
    
    termPositions = reader.termPositionsEnum(new Term("preanalyzed", "term3"));
    assertTrue(termPositions.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, termPositions.freq());
    assertEquals(2, termPositions.nextPosition());
    reader.close();
  }

  /**
   * Test adding two fields with the same name, but 
   * with different term vector setting (LUCENE-766).
   */
  public void testMixedTermVectorSettingsSameField() throws Exception {
    Document doc = new Document();
    // f1 first without tv then with tv
    doc.add(newStringField("f1", "v1", Field.Store.YES));
    FieldType customType2 = new FieldType(StringField.TYPE_STORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorOffsets(true);
    customType2.setStoreTermVectorPositions(true);
    doc.add(newField("f1", "v2", customType2));
    // f2 first with tv then without tv
    doc.add(newField("f2", "v1", customType2));
    doc.add(newStringField("f2", "v2", Field.Store.YES));

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer.addDocument(doc);
    writer.close();

    _TestUtil.checkIndex(dir);

    IndexReader reader = DirectoryReader.open(dir);
    // f1
    Terms tfv1 = reader.getTermVectors(0).terms("f1");
    assertNotNull(tfv1);
    assertEquals("the 'with_tv' setting should rule!",2,tfv1.size());
    // f2
    Terms tfv2 = reader.getTermVectors(0).terms("f2");
    assertNotNull(tfv2);
    assertEquals("the 'with_tv' setting should rule!",2,tfv2.size());
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
    customType3.setIndexOptions(IndexOptions.DOCS_ONLY);
    Field f = newField("f2", "v1", customType3);
    doc.add(f);
    doc.add(newField("f2", "v2", customType2));

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer.addDocument(doc);
    writer.forceMerge(1); // be sure to have a single segment
    writer.close();

    _TestUtil.checkIndex(dir);

    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(dir));
    FieldInfos fi = reader.getFieldInfos();
    // f1
    assertFalse("f1 should have no norms", fi.fieldInfo("f1").hasNorms());
    assertEquals("omitTermFreqAndPositions field bit should not be set for f1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f1").getIndexOptions());
    // f2
    assertTrue("f2 should have norms", fi.fieldInfo("f2").hasNorms());
    assertEquals("omitTermFreqAndPositions field bit should be set for f2", IndexOptions.DOCS_ONLY, fi.fieldInfo("f2").getIndexOptions());
    reader.close();
  }
}
