package org.apache.lucene.codecs.lucene41;

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

import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockFixedLengthPayloadFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.MockVariableLengthPayloadFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/** 
 * Tests partial enumeration (only pulling a subset of the indexed data) 
 */
public class TestBlockPostingsFormat3 extends LuceneTestCase {
  static final int MAXDOC = Lucene41PostingsFormat.BLOCK_SIZE * 20;
  
  // creates 8 fields with different options and does "duels" of fields against each other
  public void test() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new Analyzer(new Analyzer.PerFieldReuseStrategy()) {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader);
        if (fieldName.contains("payloadsFixed")) {
          TokenFilter filter = new MockFixedLengthPayloadFilter(new Random(0), tokenizer, 1);
          return new TokenStreamComponents(tokenizer, filter);
        } else if (fieldName.contains("payloadsVariable")) {
          TokenFilter filter = new MockVariableLengthPayloadFilter(new Random(0), tokenizer);
          return new TokenStreamComponents(tokenizer, filter);
        } else {
          return new TokenStreamComponents(tokenizer);
        }
      }
    };
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
    iwc.setCodec(_TestUtil.alwaysPostingsFormat(new Lucene41PostingsFormat())); 
    // TODO we could actually add more fields implemented with different PFs
    // or, just put this test into the usual rotation?
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    FieldType docsOnlyType = new FieldType(TextField.TYPE_NOT_STORED);
    // turn this on for a cross-check
    docsOnlyType.setStoreTermVectors(true);
    docsOnlyType.setIndexOptions(IndexOptions.DOCS_ONLY);
    
    FieldType docsAndFreqsType = new FieldType(TextField.TYPE_NOT_STORED);
    // turn this on for a cross-check
    docsAndFreqsType.setStoreTermVectors(true);
    docsAndFreqsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    
    FieldType positionsType = new FieldType(TextField.TYPE_NOT_STORED);
    // turn these on for a cross-check
    positionsType.setStoreTermVectors(true);
    positionsType.setStoreTermVectorPositions(true);
    positionsType.setStoreTermVectorOffsets(true);
    positionsType.setStoreTermVectorPayloads(true);
    FieldType offsetsType = new FieldType(positionsType);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field field1 = new Field("field1docs", "", docsOnlyType);
    Field field2 = new Field("field2freqs", "", docsAndFreqsType);
    Field field3 = new Field("field3positions", "", positionsType);
    Field field4 = new Field("field4offsets", "", offsetsType);
    Field field5 = new Field("field5payloadsFixed", "", positionsType);
    Field field6 = new Field("field6payloadsVariable", "", positionsType);
    Field field7 = new Field("field7payloadsFixedOffsets", "", offsetsType);
    Field field8 = new Field("field8payloadsVariableOffsets", "", offsetsType);
    doc.add(field1);
    doc.add(field2);
    doc.add(field3);
    doc.add(field4);
    doc.add(field5);
    doc.add(field6);
    doc.add(field7);
    doc.add(field8);
    for (int i = 0; i < MAXDOC; i++) {
      String stringValue = Integer.toString(i) + " verycommon " + English.intToEnglish(i).replace('-', ' ') + " " + _TestUtil.randomSimpleString(random());
      field1.setStringValue(stringValue);
      field2.setStringValue(stringValue);
      field3.setStringValue(stringValue);
      field4.setStringValue(stringValue);
      field5.setStringValue(stringValue);
      field6.setStringValue(stringValue);
      field7.setStringValue(stringValue);
      field8.setStringValue(stringValue);
      iw.addDocument(doc);
    }
    iw.close();
    verify(dir);
    _TestUtil.checkIndex(dir); // for some extra coverage, checkIndex before we forceMerge
    iwc.setOpenMode(OpenMode.APPEND);
    IndexWriter iw2 = new IndexWriter(dir, iwc);
    iw2.forceMerge(1);
    iw2.close();
    verify(dir);
    dir.close();
  }
  
  private void verify(Directory dir) throws Exception {
    DirectoryReader ir = DirectoryReader.open(dir);
    for (AtomicReaderContext leaf : ir.leaves()) {
      AtomicReader leafReader = leaf.reader();
      assertTerms(leafReader.terms("field1docs"), leafReader.terms("field2freqs"), true);
      assertTerms(leafReader.terms("field3positions"), leafReader.terms("field4offsets"), true);
      assertTerms(leafReader.terms("field4offsets"), leafReader.terms("field5payloadsFixed"), true);
      assertTerms(leafReader.terms("field5payloadsFixed"), leafReader.terms("field6payloadsVariable"), true);
      assertTerms(leafReader.terms("field6payloadsVariable"), leafReader.terms("field7payloadsFixedOffsets"), true);
      assertTerms(leafReader.terms("field7payloadsFixedOffsets"), leafReader.terms("field8payloadsVariableOffsets"), true);
    }
    ir.close();
  }
  
  // following code is almost an exact dup of code from TestDuelingCodecs: sorry!
  
  public void assertTerms(Terms leftTerms, Terms rightTerms, boolean deep) throws Exception {
    if (leftTerms == null || rightTerms == null) {
      assertNull(leftTerms);
      assertNull(rightTerms);
      return;
    }
    assertTermsStatistics(leftTerms, rightTerms);
    
    // NOTE: we don't assert hasOffsets/hasPositions/hasPayloads because they are allowed to be different

    TermsEnum leftTermsEnum = leftTerms.iterator(null);
    TermsEnum rightTermsEnum = rightTerms.iterator(null);
    assertTermsEnum(leftTermsEnum, rightTermsEnum, true);
    
    assertTermsSeeking(leftTerms, rightTerms);
    
    if (deep) {
      int numIntersections = atLeast(3);
      for (int i = 0; i < numIntersections; i++) {
        String re = AutomatonTestUtil.randomRegexp(random());
        CompiledAutomaton automaton = new CompiledAutomaton(new RegExp(re, RegExp.NONE).toAutomaton());
        if (automaton.type == CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
          // TODO: test start term too
          TermsEnum leftIntersection = leftTerms.intersect(automaton, null);
          TermsEnum rightIntersection = rightTerms.intersect(automaton, null);
          assertTermsEnum(leftIntersection, rightIntersection, rarely());
        }
      }
    }
  }
  
  private void assertTermsSeeking(Terms leftTerms, Terms rightTerms) throws Exception {
    TermsEnum leftEnum = null;
    TermsEnum rightEnum = null;
    
    // just an upper bound
    int numTests = atLeast(20);
    Random random = random();
    
    // collect this number of terms from the left side
    HashSet<BytesRef> tests = new HashSet<BytesRef>();
    int numPasses = 0;
    while (numPasses < 10 && tests.size() < numTests) {
      leftEnum = leftTerms.iterator(leftEnum);
      BytesRef term = null;
      while ((term = leftEnum.next()) != null) {
        int code = random.nextInt(10);
        if (code == 0) {
          // the term
          tests.add(BytesRef.deepCopyOf(term));
        } else if (code == 1) {
          // truncated subsequence of term
          term = BytesRef.deepCopyOf(term);
          if (term.length > 0) {
            // truncate it
            term.length = random.nextInt(term.length);
          }
        } else if (code == 2) {
          // term, but ensure a non-zero offset
          byte newbytes[] = new byte[term.length+5];
          System.arraycopy(term.bytes, term.offset, newbytes, 5, term.length);
          tests.add(new BytesRef(newbytes, 5, term.length));
        }
      }
      numPasses++;
    }
    
    ArrayList<BytesRef> shuffledTests = new ArrayList<BytesRef>(tests);
    Collections.shuffle(shuffledTests, random);
    
    for (BytesRef b : shuffledTests) {
      leftEnum = leftTerms.iterator(leftEnum);
      rightEnum = rightTerms.iterator(rightEnum);
      
      assertEquals(leftEnum.seekExact(b, false), rightEnum.seekExact(b, false));
      assertEquals(leftEnum.seekExact(b, true), rightEnum.seekExact(b, true));
      
      SeekStatus leftStatus;
      SeekStatus rightStatus;
      
      leftStatus = leftEnum.seekCeil(b, false);
      rightStatus = rightEnum.seekCeil(b, false);
      assertEquals(leftStatus, rightStatus);
      if (leftStatus != SeekStatus.END) {
        assertEquals(leftEnum.term(), rightEnum.term());
      }
      
      leftStatus = leftEnum.seekCeil(b, true);
      rightStatus = rightEnum.seekCeil(b, true);
      assertEquals(leftStatus, rightStatus);
      if (leftStatus != SeekStatus.END) {
        assertEquals(leftEnum.term(), rightEnum.term());
      }
    }
  }
  
  /** 
   * checks collection-level statistics on Terms 
   */
  public void assertTermsStatistics(Terms leftTerms, Terms rightTerms) throws Exception {
    assert leftTerms.getComparator() == rightTerms.getComparator();
    if (leftTerms.getDocCount() != -1 && rightTerms.getDocCount() != -1) {
      assertEquals(leftTerms.getDocCount(), rightTerms.getDocCount());
    }
    if (leftTerms.getSumDocFreq() != -1 && rightTerms.getSumDocFreq() != -1) {
      assertEquals(leftTerms.getSumDocFreq(), rightTerms.getSumDocFreq());
    }
    if (leftTerms.getSumTotalTermFreq() != -1 && rightTerms.getSumTotalTermFreq() != -1) {
      assertEquals(leftTerms.getSumTotalTermFreq(), rightTerms.getSumTotalTermFreq());
    }
    if (leftTerms.size() != -1 && rightTerms.size() != -1) {
      assertEquals(leftTerms.size(), rightTerms.size());
    }
  }

  /** 
   * checks the terms enum sequentially
   * if deep is false, it does a 'shallow' test that doesnt go down to the docsenums
   */
  public void assertTermsEnum(TermsEnum leftTermsEnum, TermsEnum rightTermsEnum, boolean deep) throws Exception {
    BytesRef term;
    Bits randomBits = new RandomBits(MAXDOC, random().nextDouble(), random());
    DocsAndPositionsEnum leftPositions = null;
    DocsAndPositionsEnum rightPositions = null;
    DocsEnum leftDocs = null;
    DocsEnum rightDocs = null;
    
    while ((term = leftTermsEnum.next()) != null) {
      assertEquals(term, rightTermsEnum.next());
      assertTermStats(leftTermsEnum, rightTermsEnum);
      if (deep) {
        // with payloads + off
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions),
                                   rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions));
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions),
                                   rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions));

        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions),
                                rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions));
        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions),
                                rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions));
        // with payloads only
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions, DocsAndPositionsEnum.FLAG_PAYLOADS),
                                   rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions, DocsAndPositionsEnum.FLAG_PAYLOADS));
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions, DocsAndPositionsEnum.FLAG_PAYLOADS),
                                   rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions, DocsAndPositionsEnum.FLAG_PAYLOADS));

        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions, DocsAndPositionsEnum.FLAG_PAYLOADS),
                                rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions, DocsAndPositionsEnum.FLAG_PAYLOADS));
        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions, DocsAndPositionsEnum.FLAG_PAYLOADS),
                                rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions, DocsAndPositionsEnum.FLAG_PAYLOADS));

        // with offsets only
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions, DocsAndPositionsEnum.FLAG_OFFSETS),
                                   rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions, DocsAndPositionsEnum.FLAG_OFFSETS));
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions, DocsAndPositionsEnum.FLAG_OFFSETS),
                                   rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions, DocsAndPositionsEnum.FLAG_OFFSETS));

        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions, DocsAndPositionsEnum.FLAG_OFFSETS),
                                rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions, DocsAndPositionsEnum.FLAG_OFFSETS));
        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions, DocsAndPositionsEnum.FLAG_OFFSETS),
                                rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions, DocsAndPositionsEnum.FLAG_OFFSETS));
        
        // with positions only
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions, DocsEnum.FLAG_NONE),
                                   rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions, DocsEnum.FLAG_NONE));
        assertDocsAndPositionsEnum(leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions, DocsEnum.FLAG_NONE),
                                   rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions, DocsEnum.FLAG_NONE));

        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions, DocsEnum.FLAG_NONE),
                                rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions, DocsEnum.FLAG_NONE));
        assertPositionsSkipping(leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions, DocsEnum.FLAG_NONE),
                                rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions, DocsEnum.FLAG_NONE));
        
        // with freqs:
        assertDocsEnum(leftDocs = leftTermsEnum.docs(null, leftDocs),
            rightDocs = rightTermsEnum.docs(null, rightDocs));
        assertDocsEnum(leftDocs = leftTermsEnum.docs(randomBits, leftDocs),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs));

        // w/o freqs:
        assertDocsEnum(leftDocs = leftTermsEnum.docs(null, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(null, rightDocs, DocsEnum.FLAG_NONE));
        assertDocsEnum(leftDocs = leftTermsEnum.docs(randomBits, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs, DocsEnum.FLAG_NONE));
        
        // with freqs:
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(null, leftDocs),
            rightDocs = rightTermsEnum.docs(null, rightDocs));
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(randomBits, leftDocs),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs));

        // w/o freqs:
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(null, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(null, rightDocs, DocsEnum.FLAG_NONE));
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(randomBits, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs, DocsEnum.FLAG_NONE));
      }
    }
    assertNull(rightTermsEnum.next());
  }
  
  /**
   * checks term-level statistics
   */
  public void assertTermStats(TermsEnum leftTermsEnum, TermsEnum rightTermsEnum) throws Exception {
    assertEquals(leftTermsEnum.docFreq(), rightTermsEnum.docFreq());
    if (leftTermsEnum.totalTermFreq() != -1 && rightTermsEnum.totalTermFreq() != -1) {
      assertEquals(leftTermsEnum.totalTermFreq(), rightTermsEnum.totalTermFreq());
    }
  }
  
  /**
   * checks docs + freqs + positions + payloads, sequentially
   */
  public void assertDocsAndPositionsEnum(DocsAndPositionsEnum leftDocs, DocsAndPositionsEnum rightDocs) throws Exception {
    if (leftDocs == null || rightDocs == null) {
      assertNull(leftDocs);
      assertNull(rightDocs);
      return;
    }
    assertEquals(-1, leftDocs.docID());
    assertEquals(-1, rightDocs.docID());
    int docid;
    while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(docid, rightDocs.nextDoc());
      int freq = leftDocs.freq();
      assertEquals(freq, rightDocs.freq());
      for (int i = 0; i < freq; i++) {
        assertEquals(leftDocs.nextPosition(), rightDocs.nextPosition());
        // we don't assert offsets/payloads, they are allowed to be different
      }
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
  }
  
  /**
   * checks docs + freqs, sequentially
   */
  public void assertDocsEnum(DocsEnum leftDocs, DocsEnum rightDocs) throws Exception {
    if (leftDocs == null) {
      assertNull(rightDocs);
      return;
    }
    assertEquals(-1, leftDocs.docID());
    assertEquals(-1, rightDocs.docID());
    int docid;
    while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(docid, rightDocs.nextDoc());
      // we don't assert freqs, they are allowed to be different
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
  }
  
  /**
   * checks advancing docs
   */
  public void assertDocsSkipping(int docFreq, DocsEnum leftDocs, DocsEnum rightDocs) throws Exception {
    if (leftDocs == null) {
      assertNull(rightDocs);
      return;
    }
    int docid = -1;
    int averageGap = MAXDOC / (1+docFreq);
    int skipInterval = 16;

    while (true) {
      if (random().nextBoolean()) {
        // nextDoc()
        docid = leftDocs.nextDoc();
        assertEquals(docid, rightDocs.nextDoc());
      } else {
        // advance()
        int skip = docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
        docid = leftDocs.advance(skip);
        assertEquals(docid, rightDocs.advance(skip));
      }
      
      if (docid == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      // we don't assert freqs, they are allowed to be different
    }
  }
  
  /**
   * checks advancing docs + positions
   */
  public void assertPositionsSkipping(int docFreq, DocsAndPositionsEnum leftDocs, DocsAndPositionsEnum rightDocs) throws Exception {
    if (leftDocs == null || rightDocs == null) {
      assertNull(leftDocs);
      assertNull(rightDocs);
      return;
    }
    
    int docid = -1;
    int averageGap = MAXDOC / (1+docFreq);
    int skipInterval = 16;

    while (true) {
      if (random().nextBoolean()) {
        // nextDoc()
        docid = leftDocs.nextDoc();
        assertEquals(docid, rightDocs.nextDoc());
      } else {
        // advance()
        int skip = docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
        docid = leftDocs.advance(skip);
        assertEquals(docid, rightDocs.advance(skip));
      }
      
      if (docid == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      int freq = leftDocs.freq();
      assertEquals(freq, rightDocs.freq());
      for (int i = 0; i < freq; i++) {
        assertEquals(leftDocs.nextPosition(), rightDocs.nextPosition());
        // we don't compare the payloads, its allowed that one is empty etc
      }
    }
  }
  
  private static class RandomBits implements Bits {
    FixedBitSet bits;
    
    RandomBits(int maxDoc, double pctLive, Random random) {
      bits = new FixedBitSet(maxDoc);
      for (int i = 0; i < maxDoc; i++) {
        if (random.nextDouble() <= pctLive) {        
          bits.set(i);
        }
      }
    }
    
    @Override
    public boolean get(int index) {
      return bits.get(index);
    }

    @Override
    public int length() {
      return bits.length();
    }
  }
}
