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
import java.util.Random;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestTermVectorsReader extends LuceneTestCase {
  //Must be lexicographically sorted, will do in setup, versus trying to maintain here
  private String[] testFields = {"f1", "f2", "f3", "f4"};
  private boolean[] testFieldsStorePos = {true, false, true, false};
  private boolean[] testFieldsStoreOff = {true, false, false, true};
  private String[] testTerms = {"this", "is", "a", "test"};
  private int[][] positions = new int[testTerms.length][];
  private Directory dir;
  private SegmentCommitInfo seg;
  private FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
  private static int TERM_FREQ = 3;

  private class TestToken implements Comparable<TestToken> {
    String text;
    int pos;
    int startOffset;
    int endOffset;
    @Override
    public int compareTo(TestToken other) {
      return pos - other.pos;
    }
  }

  TestToken[] tokens = new TestToken[testTerms.length * TERM_FREQ];

  @Override
  public void setUp() throws Exception {
    super.setUp();
    /*
    for (int i = 0; i < testFields.length; i++) {
      fieldInfos.add(testFields[i], true, true, testFieldsStorePos[i], testFieldsStoreOff[i]);
    }
    */

    Arrays.sort(testTerms);
    int tokenUpto = 0;
    Random rnd = random();
    for (int i = 0; i < testTerms.length; i++) {
      positions[i] = new int[TERM_FREQ];
      // first position must be 0
      for (int j = 0; j < TERM_FREQ; j++) {
        // positions are always sorted in increasing order
        positions[i][j] = (int) (j * 10 + rnd.nextDouble() * 10);
        TestToken token = tokens[tokenUpto++] = new TestToken();
        token.text = testTerms[i];
        token.pos = positions[i][j];
        token.startOffset = j * 10;
        token.endOffset = j * 10 + testTerms[i].length();
      }
    }
    Arrays.sort(tokens);

    dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(new MyAnalyzer()).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(false, 10))
            .setUseCompoundFile(false)
    );

    Document doc = new Document();
    for(int i=0;i<testFields.length;i++) {
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      if (testFieldsStorePos[i] && testFieldsStoreOff[i]) {
        customType.setStoreTermVectors(true);
        customType.setStoreTermVectorPositions(true);
        customType.setStoreTermVectorOffsets(true);
      }
      else if (testFieldsStorePos[i] && !testFieldsStoreOff[i]) {
        customType.setStoreTermVectors(true);
        customType.setStoreTermVectorPositions(true);
      }
      else if (!testFieldsStorePos[i] && testFieldsStoreOff[i]) {
        customType.setStoreTermVectors(true);
        customType.setStoreTermVectorPositions(true);
        customType.setStoreTermVectorOffsets(true);
      }
      else {
        customType.setStoreTermVectors(true);
      }
      doc.add(new Field(testFields[i], "", customType));
    }

    //Create 5 documents for testing, they all have the same
    //terms
    for(int j=0;j<5;j++) {
      writer.addDocument(doc);
    }
    writer.commit();
    seg = writer.newestSegment();
    writer.close();

    fieldInfos = IndexWriter.readFieldInfos(seg);
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  private class MyTokenizer extends Tokenizer {
    private int tokenUpto;
    
    private final CharTermAttribute termAtt;
    private final PositionIncrementAttribute posIncrAtt;
    private final OffsetAttribute offsetAtt;
    
    public MyTokenizer() {
      super();
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
    }
    
    @Override
    public boolean incrementToken() {
      if (tokenUpto >= tokens.length) {
        return false;
      } else {
        final TestToken testToken = tokens[tokenUpto++];
        clearAttributes();
        termAtt.append(testToken.text);
        offsetAtt.setOffset(testToken.startOffset, testToken.endOffset);
        if (tokenUpto > 1) {
          posIncrAtt.setPositionIncrement(testToken.pos - tokens[tokenUpto-2].pos);
        } else {
          posIncrAtt.setPositionIncrement(testToken.pos+1);
        }
        return true;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.tokenUpto = 0;
    }
  }

  private class MyAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new MyTokenizer());
    }
  }

  public void test() throws IOException {
    //Check to see the files were created properly in setup
    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext ctx : reader.leaves()) {
      SegmentReader sr = (SegmentReader) ctx.reader();
      assertTrue(sr.getFieldInfos().hasVectors());
    }
    reader.close();
  }

  public void testReader() throws IOException {
    TermVectorsReader reader = Codec.getDefault().termVectorsFormat().vectorsReader(dir, seg.info, fieldInfos, newIOContext(random()));
    for (int j = 0; j < 5; j++) {
      Terms vector = reader.get(j).terms(testFields[0]);
      assertNotNull(vector);
      assertEquals(testTerms.length, vector.size());
      TermsEnum termsEnum = vector.iterator();
      for (int i = 0; i < testTerms.length; i++) {
        final BytesRef text = termsEnum.next();
        assertNotNull(text);
        String term = text.utf8ToString();
        //System.out.println("Term: " + term);
        assertEquals(testTerms[i], term);
      }
      assertNull(termsEnum.next());
    }
    reader.close();
  }
  
  public void testDocsEnum() throws IOException {
    TermVectorsReader reader = Codec.getDefault().termVectorsFormat().vectorsReader(dir, seg.info, fieldInfos, newIOContext(random()));
    for (int j = 0; j < 5; j++) {
      Terms vector = reader.get(j).terms(testFields[0]);
      assertNotNull(vector);
      assertEquals(testTerms.length, vector.size());
      TermsEnum termsEnum = vector.iterator();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < testTerms.length; i++) {
        final BytesRef text = termsEnum.next();
        assertNotNull(text);
        String term = text.utf8ToString();
        //System.out.println("Term: " + term);
        assertEquals(testTerms[i], term);
        
        postingsEnum = TestUtil.docs(random(), termsEnum, postingsEnum, PostingsEnum.NONE);
        assertNotNull(postingsEnum);
        int doc = postingsEnum.docID();
        assertEquals(-1, doc);
        assertTrue(postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, postingsEnum.nextDoc());
      }
      assertNull(termsEnum.next());
    }
    reader.close();
  }

  public void testPositionReader() throws IOException {
    TermVectorsReader reader = Codec.getDefault().termVectorsFormat().vectorsReader(dir, seg.info, fieldInfos, newIOContext(random()));
    BytesRef[] terms;
    Terms vector = reader.get(0).terms(testFields[0]);
    assertNotNull(vector);
    assertEquals(testTerms.length, vector.size());
    TermsEnum termsEnum = vector.iterator();
    PostingsEnum dpEnum = null;
    for (int i = 0; i < testTerms.length; i++) {
      final BytesRef text = termsEnum.next();
      assertNotNull(text);
      String term = text.utf8ToString();
      //System.out.println("Term: " + term);
      assertEquals(testTerms[i], term);

      dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
      assertNotNull(dpEnum);
      int doc = dpEnum.docID();
      assertEquals(-1, doc);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(dpEnum.freq(), positions[i].length);
      for (int j = 0; j < positions[i].length; j++) {
        assertEquals(positions[i][j], dpEnum.nextPosition());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

      dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
      doc = dpEnum.docID();
      assertEquals(-1, doc);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertNotNull(dpEnum);
      assertEquals(dpEnum.freq(), positions[i].length);
      for (int j = 0; j < positions[i].length; j++) {
        assertEquals(positions[i][j], dpEnum.nextPosition());
        assertEquals(j*10, dpEnum.startOffset());
        assertEquals(j*10 + testTerms[i].length(), dpEnum.endOffset());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());
    }

    Terms freqVector = reader.get(0).terms(testFields[1]); //no pos, no offset
    assertNotNull(freqVector);
    assertEquals(testTerms.length, freqVector.size());
    termsEnum = freqVector.iterator();
    assertNotNull(termsEnum);
    for (int i = 0; i < testTerms.length; i++) {
      final BytesRef text = termsEnum.next();
      assertNotNull(text);
      String term = text.utf8ToString();
      //System.out.println("Term: " + term);
      assertEquals(testTerms[i], term);
      assertNotNull(termsEnum.postings(null));
      assertNotNull(termsEnum.postings(null, PostingsEnum.ALL));
    }
    reader.close();
  }

  public void testOffsetReader() throws IOException {
    TermVectorsReader reader = Codec.getDefault().termVectorsFormat().vectorsReader(dir, seg.info, fieldInfos, newIOContext(random()));
    Terms vector = reader.get(0).terms(testFields[0]);
    assertNotNull(vector);
    TermsEnum termsEnum = vector.iterator();
    assertNotNull(termsEnum);
    assertEquals(testTerms.length, vector.size());
    PostingsEnum dpEnum = null;
    for (int i = 0; i < testTerms.length; i++) {
      final BytesRef text = termsEnum.next();
      assertNotNull(text);
      String term = text.utf8ToString();
      assertEquals(testTerms[i], term);

      dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
      assertNotNull(dpEnum);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(dpEnum.freq(), positions[i].length);
      for (int j = 0; j < positions[i].length; j++) {
        assertEquals(positions[i][j], dpEnum.nextPosition());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

      dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertNotNull(dpEnum);
      assertEquals(dpEnum.freq(), positions[i].length);
      for (int j = 0; j < positions[i].length; j++) {
        assertEquals(positions[i][j], dpEnum.nextPosition());
        assertEquals(j*10, dpEnum.startOffset());
        assertEquals(j*10 + testTerms[i].length(), dpEnum.endOffset());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());
    }
    reader.close();
  }

  public void testIllegalIndexableField() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer a = new MockAnalyzer(random());
    a.setEnableChecks(false);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, a);
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPayloads(true);
    Document doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot index term vector payloads without term vector positions (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(false);
    ft.setStoreTermVectorOffsets(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot index term vector offsets when term vectors are not indexed (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(false);
    ft.setStoreTermVectorPositions(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot index term vector positions when term vectors are not indexed (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(false);
    ft.setStoreTermVectorPayloads(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot index term vector payloads when term vectors are not indexed (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPayloads(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot index term vector payloads without term vector positions (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(StoredField.TYPE);
    ft.setStoreTermVectors(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot store term vectors for a field that is not indexed (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(StoredField.TYPE);
    ft.setStoreTermVectorPositions(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot store term vector positions for a field that is not indexed (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(StoredField.TYPE);
    ft.setStoreTermVectorOffsets(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot store term vector offsets for a field that is not indexed (field=\"field\")", iae.getMessage());
    }

    ft = new FieldType(StoredField.TYPE);
    ft.setStoreTermVectorPayloads(true);
    doc = new Document();
    doc.add(new Field("field", "value", ft));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertEquals("cannot store term vector payloads for a field that is not indexed (field=\"field\")", iae.getMessage());
    }

    w.close();
    
    dir.close();
  }
}
