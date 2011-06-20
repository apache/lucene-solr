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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IOContext.Context;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestTermVectorsReader extends LuceneTestCase {
  //Must be lexicographically sorted, will do in setup, versus trying to maintain here
  private String[] testFields = {"f1", "f2", "f3", "f4"};
  private boolean[] testFieldsStorePos = {true, false, true, false};
  private boolean[] testFieldsStoreOff = {true, false, false, true};
  private String[] testTerms = {"this", "is", "a", "test"};
  private int[][] positions = new int[testTerms.length][];
  private TermVectorOffsetInfo[][] offsets = new TermVectorOffsetInfo[testTerms.length][];
  private Directory dir;
  private String seg;
  private FieldInfos fieldInfos = new FieldInfos();
  private static int TERM_FREQ = 3;

  private class TestToken implements Comparable<TestToken> {
    String text;
    int pos;
    int startOffset;
    int endOffset;
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
    for (int i = 0; i < testTerms.length; i++) {
      positions[i] = new int[TERM_FREQ];
      offsets[i] = new TermVectorOffsetInfo[TERM_FREQ];
      // first position must be 0
      for (int j = 0; j < TERM_FREQ; j++) {
        // positions are always sorted in increasing order
        positions[i][j] = (int) (j * 10 + Math.random() * 10);
        // offsets are always sorted in increasing order
        offsets[i][j] = new TermVectorOffsetInfo(j * 10, j * 10 + testTerms[i].length());
        TestToken token = tokens[tokenUpto++] = new TestToken();
        token.text = testTerms[i];
        token.pos = positions[i][j];
        token.startOffset = offsets[i][j].getStartOffset();
        token.endOffset = offsets[i][j].getEndOffset();
      }
    }
    Arrays.sort(tokens);

    dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MyAnalyzer()).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(false, 10))
    );

    Document doc = new Document();
    for(int i=0;i<testFields.length;i++) {
      final Field.TermVector tv;
      if (testFieldsStorePos[i] && testFieldsStoreOff[i])
        tv = Field.TermVector.WITH_POSITIONS_OFFSETS;
      else if (testFieldsStorePos[i] && !testFieldsStoreOff[i])
        tv = Field.TermVector.WITH_POSITIONS;
      else if (!testFieldsStorePos[i] && testFieldsStoreOff[i])
        tv = Field.TermVector.WITH_OFFSETS;
      else
        tv = Field.TermVector.YES;
      doc.add(new Field(testFields[i], "", Field.Store.NO, Field.Index.ANALYZED, tv));
    }

    //Create 5 documents for testing, they all have the same
    //terms
    for(int j=0;j<5;j++)
      writer.addDocument(doc);
    writer.commit();
    seg = writer.newestSegment().name;
    writer.close();

    fieldInfos = new FieldInfos(dir, IndexFileNames.segmentFileName(seg, "", IndexFileNames.FIELD_INFOS_EXTENSION));
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  private class MyTokenStream extends TokenStream {
    int tokenUpto;
    
    CharTermAttribute termAtt;
    PositionIncrementAttribute posIncrAtt;
    OffsetAttribute offsetAtt;
    
    public MyTokenStream() {
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
    }
    
    @Override
    public boolean incrementToken() {
      if (tokenUpto >= tokens.length)
        return false;
      else {
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
  }

  private class MyAnalyzer extends Analyzer {
    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new MyTokenStream();
    }
  }

  public void test() throws IOException {
    //Check to see the files were created properly in setup
    assertTrue(dir.fileExists(IndexFileNames.segmentFileName(seg, "", IndexFileNames.VECTORS_DOCUMENTS_EXTENSION)));
    assertTrue(dir.fileExists(IndexFileNames.segmentFileName(seg, "", IndexFileNames.VECTORS_INDEX_EXTENSION)));
  }

  public void testReader() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
    for (int j = 0; j < 5; j++) {
      TermFreqVector vector = reader.get(j, testFields[0]);
      assertTrue(vector != null);
      BytesRef[] terms = vector.getTerms();
      assertTrue(terms != null);
      assertTrue(terms.length == testTerms.length);
      for (int i = 0; i < terms.length; i++) {
        String term = terms[i].utf8ToString();
        //System.out.println("Term: " + term);
        assertTrue(term.equals(testTerms[i]));
      }
    }
    reader.close();
  }

  public void testPositionReader() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
    TermPositionVector vector;
    BytesRef[] terms;
    vector = (TermPositionVector) reader.get(0, testFields[0]);
    assertTrue(vector != null);
    terms = vector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i].utf8ToString();
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));
      int[] positions = vector.getTermPositions(i);
      assertTrue(positions != null);
      assertTrue(positions.length == this.positions[i].length);
      for (int j = 0; j < positions.length; j++) {
        int position = positions[j];
        assertTrue(position == this.positions[i][j]);
      }
      TermVectorOffsetInfo[] offset = vector.getOffsets(i);
      assertTrue(offset != null);
      assertTrue(offset.length == this.offsets[i].length);
      for (int j = 0; j < offset.length; j++) {
        TermVectorOffsetInfo termVectorOffsetInfo = offset[j];
        assertTrue(termVectorOffsetInfo.equals(offsets[i][j]));
      }
    }

    TermFreqVector freqVector = reader.get(0, testFields[1]); //no pos, no offset
    assertTrue(freqVector != null);
    assertTrue(freqVector instanceof TermPositionVector == false);
    terms = freqVector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i].utf8ToString();
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));
    }
    reader.close();
  }

  public void testOffsetReader() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
    TermPositionVector vector = (TermPositionVector) reader.get(0, testFields[0]);
    assertTrue(vector != null);
    BytesRef[] terms = vector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i].utf8ToString();
      //System.out.println("Term: " + term);
      assertTrue(term.equals(testTerms[i]));
      int[] positions = vector.getTermPositions(i);
      assertTrue(positions != null);
      assertTrue(positions.length == this.positions[i].length);
      for (int j = 0; j < positions.length; j++) {
        int position = positions[j];
        assertTrue(position == this.positions[i][j]);
      }
      TermVectorOffsetInfo[] offset = vector.getOffsets(i);
      assertTrue(offset != null);
      assertTrue(offset.length == this.offsets[i].length);
      for (int j = 0; j < offset.length; j++) {
        TermVectorOffsetInfo termVectorOffsetInfo = offset[j];
        assertTrue(termVectorOffsetInfo.equals(offsets[i][j]));
      }
    }
    reader.close();
  }

  public void testMapper() throws IOException {
    TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
    SortedTermVectorMapper mapper = new SortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
    reader.get(0, mapper);
    SortedSet<TermVectorEntry> set = mapper.getTermVectorEntrySet();
    assertTrue("set is null and it shouldn't be", set != null);
    //three fields, 4 terms, all terms are the same
    assertTrue("set Size: " + set.size() + " is not: " + 4, set.size() == 4);
    //Check offsets and positions
    for (Iterator<TermVectorEntry> iterator = set.iterator(); iterator.hasNext();) {
      TermVectorEntry tve =  iterator.next();
      assertTrue("tve is null and it shouldn't be", tve != null);
      assertTrue("tve.getOffsets() is null and it shouldn't be", tve.getOffsets() != null);
      assertTrue("tve.getPositions() is null and it shouldn't be", tve.getPositions() != null);

    }

    mapper = new SortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
    reader.get(1, mapper);
    set = mapper.getTermVectorEntrySet();
    assertTrue("set is null and it shouldn't be", set != null);
    //three fields, 4 terms, all terms are the same
    assertTrue("set Size: " + set.size() + " is not: " + 4, set.size() == 4);
    //Should have offsets and positions b/c we are munging all the fields together
    for (Iterator<TermVectorEntry> iterator = set.iterator(); iterator.hasNext();) {
      TermVectorEntry tve = iterator.next();
      assertTrue("tve is null and it shouldn't be", tve != null);
      assertTrue("tve.getOffsets() is null and it shouldn't be", tve.getOffsets() != null);
      assertTrue("tve.getPositions() is null and it shouldn't be", tve.getPositions() != null);

    }


    FieldSortedTermVectorMapper fsMapper = new FieldSortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
    reader.get(0, fsMapper);
    Map<String,SortedSet<TermVectorEntry>> map = fsMapper.getFieldToTerms();
    assertTrue("map Size: " + map.size() + " is not: " + testFields.length, map.size() == testFields.length);
    for (Map.Entry<String,SortedSet<TermVectorEntry>> entry : map.entrySet()) {
      SortedSet<TermVectorEntry> sortedSet =  entry.getValue();
      assertTrue("sortedSet Size: " + sortedSet.size() + " is not: " + 4, sortedSet.size() == 4);
      for (final TermVectorEntry tve : sortedSet) {
        assertTrue("tve is null and it shouldn't be", tve != null);
        //Check offsets and positions.
        assertTrue("tve is null and it shouldn't be", tve != null);
        String field = tve.getField();
        if (field.equals(testFields[0])) {
          //should have offsets

          assertTrue("tve.getOffsets() is null and it shouldn't be", tve.getOffsets() != null);
          assertTrue("tve.getPositions() is null and it shouldn't be", tve.getPositions() != null);
        }
        else if (field.equals(testFields[1])) {
          //should not have offsets

          assertTrue("tve.getOffsets() is not null and it shouldn't be", tve.getOffsets() == null);
          assertTrue("tve.getPositions() is not null and it shouldn't be", tve.getPositions() == null);
        }
      }
    }
    //Try mapper that ignores offs and positions
    fsMapper = new FieldSortedTermVectorMapper(true, true, new TermVectorEntryFreqSortedComparator());
    reader.get(0, fsMapper);
    map = fsMapper.getFieldToTerms();
    assertTrue("map Size: " + map.size() + " is not: " + testFields.length, map.size() == testFields.length);
    for (final Map.Entry<String,SortedSet<TermVectorEntry>> entry : map.entrySet()) {
      SortedSet<TermVectorEntry> sortedSet =  entry.getValue();
      assertTrue("sortedSet Size: " + sortedSet.size() + " is not: " + 4, sortedSet.size() == 4);
      for (final TermVectorEntry tve : sortedSet) {
        assertTrue("tve is null and it shouldn't be", tve != null);
        //Check offsets and positions.
        assertTrue("tve is null and it shouldn't be", tve != null);
        String field = tve.getField();
        if (field.equals(testFields[0])) {
          //should have offsets

          assertTrue("tve.getOffsets() is null and it shouldn't be", tve.getOffsets() == null);
          assertTrue("tve.getPositions() is null and it shouldn't be", tve.getPositions() == null);
        }
        else if (field.equals(testFields[1])) {
          //should not have offsets

          assertTrue("tve.getOffsets() is not null and it shouldn't be", tve.getOffsets() == null);
          assertTrue("tve.getPositions() is not null and it shouldn't be", tve.getPositions() == null);
        }
      }
    }

    // test setDocumentNumber()
    IndexReader ir = IndexReader.open(dir, true);
    DocNumAwareMapper docNumAwareMapper = new DocNumAwareMapper();
    assertEquals(-1, docNumAwareMapper.getDocumentNumber());

    ir.getTermFreqVector(0, docNumAwareMapper);
    assertEquals(0, docNumAwareMapper.getDocumentNumber());
    docNumAwareMapper.setDocumentNumber(-1);

    ir.getTermFreqVector(1, docNumAwareMapper);
    assertEquals(1, docNumAwareMapper.getDocumentNumber());
    docNumAwareMapper.setDocumentNumber(-1);

    ir.getTermFreqVector(0, "f1", docNumAwareMapper);
    assertEquals(0, docNumAwareMapper.getDocumentNumber());
    docNumAwareMapper.setDocumentNumber(-1);

    ir.getTermFreqVector(1, "f2", docNumAwareMapper);
    assertEquals(1, docNumAwareMapper.getDocumentNumber());
    docNumAwareMapper.setDocumentNumber(-1);

    ir.getTermFreqVector(0, "f1", docNumAwareMapper);
    assertEquals(0, docNumAwareMapper.getDocumentNumber());

    ir.close();
    reader.close();
  }


  /**
   * Make sure exceptions and bad params are handled appropriately
   */
  public void testBadParams() throws IOException {
    TermVectorsReader reader = null;
    try {
      reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
      //Bad document number, good field number
      reader.get(50, testFields[0]);
      fail();
    } catch (IOException e) {
      // expected exception
    } finally {
      reader.close();
    }
    try {
      reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
      //Bad document number, no field
      reader.get(50);
      fail();
    } catch (IOException e) {
      // expected exception
    } finally {
      reader.close();
    }
    try {
      reader = new TermVectorsReader(dir, seg, fieldInfos, IOContext.READ);
      //good document number, bad field number
      TermFreqVector vector = reader.get(0, "f50");
      assertTrue(vector == null);
      reader.close();
    } catch (IOException e) {
      fail();
    } finally {
      reader.close();
    }
  }


  public static class DocNumAwareMapper extends TermVectorMapper {

    public DocNumAwareMapper() {
    }

    private int documentNumber = -1;

    @Override
    public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
      if (documentNumber == -1) {
        throw new RuntimeException("Documentnumber should be set at this point!");
      }
    }

    @Override
    public void map(BytesRef term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
      if (documentNumber == -1) {
        throw new RuntimeException("Documentnumber should be set at this point!");
      }
    }

    public int getDocumentNumber() {
      return documentNumber;
    }

    @Override
    public void setDocumentNumber(int documentNumber) {
      this.documentNumber = documentNumber;
    }
  }
}
