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
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestDocumentWriter extends LuceneTestCase {
  private RAMDirectory dir;

  public TestDocumentWriter(String s) {
    super(s);
  }

  protected void setUp() throws Exception {
    super.setUp();
    dir = new RAMDirectory();
  }

  public void test() {
    assertTrue(dir != null);
  }

  public void testAddDocument() throws Exception {
    Document testDoc = new Document();
    DocHelper.setupDoc(testDoc);
    Analyzer analyzer = new WhitespaceAnalyzer();
    IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.addDocument(testDoc);
    writer.flush();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    //After adding the document, we should be able to read it back in
    SegmentReader reader = SegmentReader.get(info);
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
    for (int i = 0; i < reader.fieldInfos.size(); i++) {
      FieldInfo fi = reader.fieldInfos.fieldInfo(i);
      if (fi.isIndexed) {
        assertTrue(fi.omitNorms == !reader.hasNorms(fi.name));
      }
    }
  }

  public void testPositionIncrementGap() throws IOException {
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new WhitespaceTokenizer(reader);
      }

      public int getPositionIncrementGap(String fieldName) {
        return 500;
      }
    };

    IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

    Document doc = new Document();
    doc.add(new Field("repeated", "repeated one", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("repeated", "repeated two", Field.Store.YES, Field.Index.ANALYZED));

    writer.addDocument(doc);
    writer.flush();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = SegmentReader.get(info);

    TermPositions termPositions = reader.termPositions(new Term("repeated", "repeated"));
    assertTrue(termPositions.next());
    int freq = termPositions.freq();
    assertEquals(2, freq);
    assertEquals(0, termPositions.nextPosition());
    assertEquals(502, termPositions.nextPosition());
  }

  public void testTokenReuse() throws IOException {
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new TokenFilter(new WhitespaceTokenizer(reader)) {
          boolean first=true;
          Token buffered;

          public Token next(final Token reusableToken) throws IOException {
            if (buffered != null) {
              Token nextToken = buffered;
              buffered=null;
              return nextToken;
            }
            Token nextToken = input.next(reusableToken);
            if (nextToken==null) return null;
            if (Character.isDigit(nextToken.termBuffer()[0])) {
              nextToken.setPositionIncrement(nextToken.termBuffer()[0] - '0');
            }
            if (first) {
              // set payload on first position only
              nextToken.setPayload(new Payload(new byte[]{100}));
              first = false;
            }

            // index a "synonym" for every token
            buffered = (Token)nextToken.clone();
            buffered.setPayload(null);
            buffered.setPositionIncrement(0);
            buffered.setTermBuffer(new char[]{'b'}, 0, 1);

            return nextToken;
          }
        };
      }
    };

    IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

    Document doc = new Document();
    doc.add(new Field("f1", "a 5 a a", Field.Store.YES, Field.Index.ANALYZED));

    writer.addDocument(doc);
    writer.flush();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = SegmentReader.get(info);

    TermPositions termPositions = reader.termPositions(new Term("f1", "a"));
    assertTrue(termPositions.next());
    int freq = termPositions.freq();
    assertEquals(3, freq);
    assertEquals(0, termPositions.nextPosition());
    assertEquals(true, termPositions.isPayloadAvailable());
    assertEquals(6, termPositions.nextPosition());
    assertEquals(false, termPositions.isPayloadAvailable());
    assertEquals(7, termPositions.nextPosition());
    assertEquals(false, termPositions.isPayloadAvailable());
  }


  public void testPreAnalyzedField() throws IOException {
    IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    
    doc.add(new Field("preanalyzed", new TokenStream() {
      private String[] tokens = new String[] {"term1", "term2", "term3", "term2"};
      private int index = 0;
      
      public Token next(final Token reusableToken) throws IOException {
        assert reusableToken != null;
        if (index == tokens.length) {
          return null;
        } else {
          return reusableToken.reinit(tokens[index++], 0, 0);
        }        
      }
      
    }, TermVector.NO));
    
    writer.addDocument(doc);
    writer.flush();
    SegmentInfo info = writer.newestSegment();
    writer.close();
    SegmentReader reader = SegmentReader.get(info);

    TermPositions termPositions = reader.termPositions(new Term("preanalyzed", "term1"));
    assertTrue(termPositions.next());
    assertEquals(1, termPositions.freq());
    assertEquals(0, termPositions.nextPosition());

    termPositions.seek(new Term("preanalyzed", "term2"));
    assertTrue(termPositions.next());
    assertEquals(2, termPositions.freq());
    assertEquals(1, termPositions.nextPosition());
    assertEquals(3, termPositions.nextPosition());
    
    termPositions.seek(new Term("preanalyzed", "term3"));
    assertTrue(termPositions.next());
    assertEquals(1, termPositions.freq());
    assertEquals(2, termPositions.nextPosition());

  }

  /**
   * Test adding two fields with the same name, but 
   * with different term vector setting (LUCENE-766).
   */
  public void testMixedTermVectorSettingsSameField() throws Exception {
    Document doc = new Document();
    // f1 first without tv then with tv
    doc.add(new Field("f1", "v1", Store.YES, Index.NOT_ANALYZED, TermVector.NO));
    doc.add(new Field("f1", "v2", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    // f2 first with tv then without tv
    doc.add(new Field("f2", "v1", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("f2", "v2", Store.YES, Index.NOT_ANALYZED, TermVector.NO));

    RAMDirectory ram = new RAMDirectory();
    IndexWriter writer = new IndexWriter(ram, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.addDocument(doc);
    writer.close();

    _TestUtil.checkIndex(ram);

    IndexReader reader = IndexReader.open(ram);
    // f1
    TermFreqVector tfv1 = reader.getTermFreqVector(0, "f1");
    assertNotNull(tfv1);
    assertEquals("the 'with_tv' setting should rule!",2,tfv1.getTerms().length);
    // f2
    TermFreqVector tfv2 = reader.getTermFreqVector(0, "f2");
    assertNotNull(tfv2);
    assertEquals("the 'with_tv' setting should rule!",2,tfv2.getTerms().length);
  }
}
