package org.apache.lucene.index;

/**
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.Fieldable;
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.addDocument(testDoc);
    writer.commit();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    //After adding the document, we should be able to read it back in
    SegmentReader reader = SegmentReader.get(true, info, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);
    assertTrue(reader != null);
    Document doc = reader.document(0);
    assertTrue(doc != null);

    //System.out.println("Document: " + doc);
    Fieldable [] fields = doc.getFields("textField2");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_2_TEXT));
    assertTrue(fields[0].isTermVectorStored());

    fields = doc.getFields("textField1");
    assertTrue(fields != null && fields.length == 1);
    assertTrue(fields[0].stringValue().equals(DocHelper.FIELD_1_TEXT));
    assertFalse(fields[0].isTermVectorStored());

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
    for (FieldInfo fi : reader.core.fieldInfos) {
      if (fi.isIndexed) {
        assertTrue(fi.omitNorms == !reader.hasNorms(fi.name));
      }
    }
    reader.close();
  }

  public void testPositionIncrementGap() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        return 500;
      }
    };

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));

    Document doc = new Document();
    doc.add(newField("repeated", "repeated one", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(newField("repeated", "repeated two", Field.Store.YES, Field.Index.ANALYZED));

    writer.addDocument(doc);
    writer.commit();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = SegmentReader.get(true, info, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);

    DocsAndPositionsEnum termPositions = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader),
                                                                          "repeated", new BytesRef("repeated"));
    assertTrue(termPositions.nextDoc() != termPositions.NO_MORE_DOCS);
    int freq = termPositions.freq();
    assertEquals(2, freq);
    assertEquals(0, termPositions.nextPosition());
    assertEquals(502, termPositions.nextPosition());
    reader.close();
  }

  public void testTokenReuse() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new TokenFilter(new MockTokenizer(reader, MockTokenizer.WHITESPACE, false)) {
          boolean first=true;
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
              payloadAtt.setPayload(new Payload(new byte[]{100}));
              first = false;
            }

            // index a "synonym" for every token
            state = captureState();
            return true;

          }

          CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
          PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
          PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);          
        };
      }
    };

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer));

    Document doc = new Document();
    doc.add(newField("f1", "a 5 a a", Field.Store.YES, Field.Index.ANALYZED));

    writer.addDocument(doc);
    writer.commit();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = SegmentReader.get(true, info, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);

    DocsAndPositionsEnum termPositions = reader.fields().terms("f1").docsAndPositions(reader.getLiveDocs(), new BytesRef("a"), null);
    assertTrue(termPositions.nextDoc() != termPositions.NO_MORE_DOCS);
    int freq = termPositions.freq();
    assertEquals(3, freq);
    assertEquals(0, termPositions.nextPosition());
    assertEquals(true, termPositions.hasPayload());
    assertEquals(6, termPositions.nextPosition());
    assertEquals(false, termPositions.hasPayload());
    assertEquals(7, termPositions.nextPosition());
    assertEquals(false, termPositions.hasPayload());
    reader.close();
  }


  public void testPreAnalyzedField() throws IOException {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document doc = new Document();
    
    doc.add(new Field("preanalyzed", new TokenStream() {
      private String[] tokens = new String[] {"term1", "term2", "term3", "term2"};
      private int index = 0;
      
      private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      
      @Override
      public boolean incrementToken() throws IOException {
        if (index == tokens.length) {
          return false;
        } else {
          clearAttributes();
          termAtt.setEmpty().append(tokens[index++]);
          return true;
        }        
      }
      
    }, TermVector.NO));
    
    writer.addDocument(doc);
    writer.commit();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = SegmentReader.get(true, info, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR);

    DocsAndPositionsEnum termPositions = reader.fields().terms("preanalyzed").docsAndPositions(reader.getLiveDocs(), new BytesRef("term1"), null);
    assertTrue(termPositions.nextDoc() != termPositions.NO_MORE_DOCS);
    assertEquals(1, termPositions.freq());
    assertEquals(0, termPositions.nextPosition());

    termPositions = reader.fields().terms("preanalyzed").docsAndPositions(reader.getLiveDocs(), new BytesRef("term2"), null);
    assertTrue(termPositions.nextDoc() != termPositions.NO_MORE_DOCS);
    assertEquals(2, termPositions.freq());
    assertEquals(1, termPositions.nextPosition());
    assertEquals(3, termPositions.nextPosition());
    
    termPositions = reader.fields().terms("preanalyzed").docsAndPositions(reader.getLiveDocs(), new BytesRef("term3"), null);
    assertTrue(termPositions.nextDoc() != termPositions.NO_MORE_DOCS);
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
    doc.add(newField("f1", "v1", Store.YES, Index.NOT_ANALYZED, TermVector.NO));
    doc.add(newField("f1", "v2", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    // f2 first with tv then without tv
    doc.add(newField("f2", "v1", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(newField("f2", "v2", Store.YES, Index.NOT_ANALYZED, TermVector.NO));

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.addDocument(doc);
    writer.close();

    _TestUtil.checkIndex(dir);

    IndexReader reader = IndexReader.open(dir, true);
    // f1
    TermFreqVector tfv1 = reader.getTermFreqVector(0, "f1");
    assertNotNull(tfv1);
    assertEquals("the 'with_tv' setting should rule!",2,tfv1.getTerms().length);
    // f2
    TermFreqVector tfv2 = reader.getTermFreqVector(0, "f2");
    assertNotNull(tfv2);
    assertEquals("the 'with_tv' setting should rule!",2,tfv2.getTerms().length);
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
    doc.add(newField("f1", "v1", Store.NO, Index.ANALYZED_NO_NORMS));
    doc.add(newField("f1", "v2", Store.YES, Index.NO));
    // f2 has no TF
    Field f = newField("f2", "v1", Store.NO, Index.ANALYZED);
    f.setOmitTermFreqAndPositions(true);
    doc.add(f);
    doc.add(newField("f2", "v2", Store.YES, Index.NO));

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.addDocument(doc);
    writer.optimize(); // be sure to have a single segment
    writer.close();

    _TestUtil.checkIndex(dir);

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(dir, false));
    FieldInfos fi = reader.fieldInfos();
    // f1
    assertFalse("f1 should have no norms", reader.hasNorms("f1"));
    assertFalse("omitTermFreqAndPositions field bit should not be set for f1", fi.fieldInfo("f1").omitTermFreqAndPositions);
    // f2
    assertTrue("f2 should have norms", reader.hasNorms("f2"));
    assertTrue("omitTermFreqAndPositions field bit should be set for f2", fi.fieldInfo("f2").omitTermFreqAndPositions);
    reader.close();
  }
}
