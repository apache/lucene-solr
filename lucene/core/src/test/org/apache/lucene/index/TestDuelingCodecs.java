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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Compares one codec against another
 */
public class TestDuelingCodecs extends LuceneTestCase {
  private Directory leftDir;
  private IndexReader leftReader;
  private Codec leftCodec;

  private Directory rightDir;
  private IndexReader rightReader;
  private Codec rightCodec;
  
  private String info;  // for debugging

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // for now its SimpleText vs Lucene41(random postings format)
    // as this gives the best overall coverage. when we have more
    // codecs we should probably pick 2 from Codec.availableCodecs()
    
    leftCodec = Codec.forName("SimpleText");
    rightCodec = new RandomCodec(random());

    leftDir = newDirectory();
    rightDir = newDirectory();

    long seed = random().nextLong();

    // must use same seed because of random payloads, etc
    Analyzer leftAnalyzer = new MockAnalyzer(new Random(seed));
    Analyzer rightAnalyzer = new MockAnalyzer(new Random(seed));
    
    // but these can be different
    // TODO: this turns this into a really big test of Multi*, is that what we want?
    IndexWriterConfig leftConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, leftAnalyzer);
    leftConfig.setCodec(leftCodec);
    // preserve docids
    leftConfig.setMergePolicy(newLogMergePolicy());

    IndexWriterConfig rightConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, rightAnalyzer);
    rightConfig.setCodec(rightCodec);
    // preserve docids
    rightConfig.setMergePolicy(newLogMergePolicy());

    // must use same seed because of random docvalues fields, etc
    RandomIndexWriter leftWriter = new RandomIndexWriter(new Random(seed), leftDir, leftConfig);
    RandomIndexWriter rightWriter = new RandomIndexWriter(new Random(seed), rightDir, rightConfig);
    
    int numdocs = atLeast(100);
    createRandomIndex(numdocs, leftWriter, seed);
    createRandomIndex(numdocs, rightWriter, seed);

    leftReader = maybeWrapReader(leftWriter.getReader());
    leftWriter.close();
    rightReader = maybeWrapReader(rightWriter.getReader());
    rightWriter.close();
    
    info = "left: " + leftCodec.toString() + " / right: " + rightCodec.toString();
  }
  
  @Override
  public void tearDown() throws Exception {
    if (leftReader != null) {
      leftReader.close();
    }
    if (rightReader != null) {
      rightReader.close();   
    }

    if (leftDir != null) {
      leftDir.close();
    }
    if (rightDir != null) {
      rightDir.close();
    }
    
    super.tearDown();
  }
  
  /**
   * populates a writer with random stuff. this must be fully reproducable with the seed!
   */
  public static void createRandomIndex(int numdocs, RandomIndexWriter writer, long seed) throws IOException {
    Random random = new Random(seed);
    // primary source for our data is from linefiledocs, its realistic.
    LineFileDocs lineFileDocs = new LineFileDocs(random);

    // TODO: we should add other fields that use things like docs&freqs but omit positions,
    // because linefiledocs doesn't cover all the possibilities.
    for (int i = 0; i < numdocs; i++) {
      writer.addDocument(lineFileDocs.nextDoc());
    }
    
    lineFileDocs.close();
  }
  
  /**
   * checks the two indexes are equivalent
   */
  public void testEquals() throws Exception {
    assertReaderStatistics(leftReader, rightReader);
    assertFields(MultiFields.getFields(leftReader), MultiFields.getFields(rightReader), true);
    assertNorms(leftReader, rightReader);
    assertStoredFields(leftReader, rightReader);
    assertTermVectors(leftReader, rightReader);
    assertDocValues(leftReader, rightReader);
    assertDeletedDocs(leftReader, rightReader);
    assertFieldInfos(leftReader, rightReader);
  }
  
  /** 
   * checks that reader-level statistics are the same 
   */
  public void assertReaderStatistics(IndexReader leftReader, IndexReader rightReader) throws Exception {
    // Somewhat redundant: we never delete docs
    assertEquals(info, leftReader.maxDoc(), rightReader.maxDoc());
    assertEquals(info, leftReader.numDocs(), rightReader.numDocs());
    assertEquals(info, leftReader.numDeletedDocs(), rightReader.numDeletedDocs());
    assertEquals(info, leftReader.hasDeletions(), rightReader.hasDeletions());
  }
  
  /** 
   * Fields api equivalency 
   */
  public void assertFields(Fields leftFields, Fields rightFields, boolean deep) throws Exception {
    // Fields could be null if there are no postings,
    // but then it must be null for both
    if (leftFields == null || rightFields == null) {
      assertNull(info, leftFields);
      assertNull(info, rightFields);
      return;
    }
    assertFieldStatistics(leftFields, rightFields);
    
    Iterator<String> leftEnum = leftFields.iterator();
    Iterator<String> rightEnum = rightFields.iterator();
    
    while (leftEnum.hasNext()) {
      String field = leftEnum.next();
      assertEquals(info, field, rightEnum.next());
      assertTerms(leftFields.terms(field), rightFields.terms(field), deep);
    }
    assertFalse(rightEnum.hasNext());
  }
  
  /** 
   * checks that top-level statistics on Fields are the same 
   */
  public void assertFieldStatistics(Fields leftFields, Fields rightFields) throws Exception {
    if (leftFields.size() != -1 && rightFields.size() != -1) {
      assertEquals(info, leftFields.size(), rightFields.size());
    }
  }
  
  /** 
   * Terms api equivalency 
   */
  public void assertTerms(Terms leftTerms, Terms rightTerms, boolean deep) throws Exception {
    if (leftTerms == null || rightTerms == null) {
      assertNull(info, leftTerms);
      assertNull(info, rightTerms);
      return;
    }
    assertTermsStatistics(leftTerms, rightTerms);
    assertEquals(leftTerms.hasOffsets(), rightTerms.hasOffsets());
    assertEquals(leftTerms.hasPositions(), rightTerms.hasPositions());
    assertEquals(leftTerms.hasPayloads(), rightTerms.hasPayloads());

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
      
      assertEquals(info, leftEnum.seekExact(b, false), rightEnum.seekExact(b, false));
      assertEquals(info, leftEnum.seekExact(b, true), rightEnum.seekExact(b, true));
      
      SeekStatus leftStatus;
      SeekStatus rightStatus;
      
      leftStatus = leftEnum.seekCeil(b, false);
      rightStatus = rightEnum.seekCeil(b, false);
      assertEquals(info, leftStatus, rightStatus);
      if (leftStatus != SeekStatus.END) {
        assertEquals(info, leftEnum.term(), rightEnum.term());
      }
      
      leftStatus = leftEnum.seekCeil(b, true);
      rightStatus = rightEnum.seekCeil(b, true);
      assertEquals(info, leftStatus, rightStatus);
      if (leftStatus != SeekStatus.END) {
        assertEquals(info, leftEnum.term(), rightEnum.term());
      }
    }
  }
  
  /** 
   * checks collection-level statistics on Terms 
   */
  public void assertTermsStatistics(Terms leftTerms, Terms rightTerms) throws Exception {
    assert leftTerms.getComparator() == rightTerms.getComparator();
    if (leftTerms.getDocCount() != -1 && rightTerms.getDocCount() != -1) {
      assertEquals(info, leftTerms.getDocCount(), rightTerms.getDocCount());
    }
    if (leftTerms.getSumDocFreq() != -1 && rightTerms.getSumDocFreq() != -1) {
      assertEquals(info, leftTerms.getSumDocFreq(), rightTerms.getSumDocFreq());
    }
    if (leftTerms.getSumTotalTermFreq() != -1 && rightTerms.getSumTotalTermFreq() != -1) {
      assertEquals(info, leftTerms.getSumTotalTermFreq(), rightTerms.getSumTotalTermFreq());
    }
    if (leftTerms.size() != -1 && rightTerms.size() != -1) {
      assertEquals(info, leftTerms.size(), rightTerms.size());
    }
  }

  /** 
   * checks the terms enum sequentially
   * if deep is false, it does a 'shallow' test that doesnt go down to the docsenums
   */
  public void assertTermsEnum(TermsEnum leftTermsEnum, TermsEnum rightTermsEnum, boolean deep) throws Exception {
    BytesRef term;
    Bits randomBits = new RandomBits(leftReader.maxDoc(), random().nextDouble(), random());
    DocsAndPositionsEnum leftPositions = null;
    DocsAndPositionsEnum rightPositions = null;
    DocsEnum leftDocs = null;
    DocsEnum rightDocs = null;
    
    while ((term = leftTermsEnum.next()) != null) {
      assertEquals(info, term, rightTermsEnum.next());
      assertTermStats(leftTermsEnum, rightTermsEnum);
      if (deep) {
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

        // with freqs:
        assertDocsEnum(leftDocs = leftTermsEnum.docs(null, leftDocs),
            rightDocs = rightTermsEnum.docs(null, rightDocs),
            true);
        assertDocsEnum(leftDocs = leftTermsEnum.docs(randomBits, leftDocs),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs),
            true);

        // w/o freqs:
        assertDocsEnum(leftDocs = leftTermsEnum.docs(null, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(null, rightDocs, DocsEnum.FLAG_NONE),
            false);
        assertDocsEnum(leftDocs = leftTermsEnum.docs(randomBits, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs, DocsEnum.FLAG_NONE),
            false);
        
        // with freqs:
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(null, leftDocs),
            rightDocs = rightTermsEnum.docs(null, rightDocs),
            true);
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(randomBits, leftDocs),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs),
            true);

        // w/o freqs:
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(null, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(null, rightDocs, DocsEnum.FLAG_NONE),
            false);
        assertDocsSkipping(leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(randomBits, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs, DocsEnum.FLAG_NONE),
            false);
      }
    }
    assertNull(info, rightTermsEnum.next());
  }
  
  /**
   * checks term-level statistics
   */
  public void assertTermStats(TermsEnum leftTermsEnum, TermsEnum rightTermsEnum) throws Exception {
    assertEquals(info, leftTermsEnum.docFreq(), rightTermsEnum.docFreq());
    if (leftTermsEnum.totalTermFreq() != -1 && rightTermsEnum.totalTermFreq() != -1) {
      assertEquals(info, leftTermsEnum.totalTermFreq(), rightTermsEnum.totalTermFreq());
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
    assertTrue(info, leftDocs.docID() == -1 || leftDocs.docID() == DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(info, rightDocs.docID() == -1 || rightDocs.docID() == DocIdSetIterator.NO_MORE_DOCS);
    int docid;
    while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(info, docid, rightDocs.nextDoc());
      int freq = leftDocs.freq();
      assertEquals(info, freq, rightDocs.freq());
      for (int i = 0; i < freq; i++) {
        assertEquals(info, leftDocs.nextPosition(), rightDocs.nextPosition());
        assertEquals(info, leftDocs.getPayload(), rightDocs.getPayload());
        assertEquals(info, leftDocs.startOffset(), rightDocs.startOffset());
        assertEquals(info, leftDocs.endOffset(), rightDocs.endOffset());
      }
    }
    assertEquals(info, DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
  }
  
  /**
   * checks docs + freqs, sequentially
   */
  public void assertDocsEnum(DocsEnum leftDocs, DocsEnum rightDocs, boolean hasFreqs) throws Exception {
    if (leftDocs == null) {
      assertNull(rightDocs);
      return;
    }
    assertTrue(info, leftDocs.docID() == -1 || leftDocs.docID() == DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(info, rightDocs.docID() == -1 || rightDocs.docID() == DocIdSetIterator.NO_MORE_DOCS);
    int docid;
    while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(info, docid, rightDocs.nextDoc());
      if (hasFreqs) {
        assertEquals(info, leftDocs.freq(), rightDocs.freq());
      }
    }
    assertEquals(info, DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
  }
  
  /**
   * checks advancing docs
   */
  public void assertDocsSkipping(int docFreq, DocsEnum leftDocs, DocsEnum rightDocs, boolean hasFreqs) throws Exception {
    if (leftDocs == null) {
      assertNull(rightDocs);
      return;
    }
    int docid = -1;
    int averageGap = leftReader.maxDoc() / (1+docFreq);
    int skipInterval = 16;

    while (true) {
      if (random().nextBoolean()) {
        // nextDoc()
        docid = leftDocs.nextDoc();
        assertEquals(info, docid, rightDocs.nextDoc());
      } else {
        // advance()
        int skip = docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
        docid = leftDocs.advance(skip);
        assertEquals(info, docid, rightDocs.advance(skip));
      }
      
      if (docid == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      if (hasFreqs) {
        assertEquals(info, leftDocs.freq(), rightDocs.freq());
      }
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
    int averageGap = leftReader.maxDoc() / (1+docFreq);
    int skipInterval = 16;

    while (true) {
      if (random().nextBoolean()) {
        // nextDoc()
        docid = leftDocs.nextDoc();
        assertEquals(info, docid, rightDocs.nextDoc());
      } else {
        // advance()
        int skip = docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
        docid = leftDocs.advance(skip);
        assertEquals(info, docid, rightDocs.advance(skip));
      }
      
      if (docid == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      int freq = leftDocs.freq();
      assertEquals(info, freq, rightDocs.freq());
      for (int i = 0; i < freq; i++) {
        assertEquals(info, leftDocs.nextPosition(), rightDocs.nextPosition());
        assertEquals(info, leftDocs.getPayload(), rightDocs.getPayload());
      }
    }
  }
  
  /** 
   * checks that norms are the same across all fields 
   */
  public void assertNorms(IndexReader leftReader, IndexReader rightReader) throws Exception {
    Fields leftFields = MultiFields.getFields(leftReader);
    Fields rightFields = MultiFields.getFields(rightReader);
    // Fields could be null if there are no postings,
    // but then it must be null for both
    if (leftFields == null || rightFields == null) {
      assertNull(info, leftFields);
      assertNull(info, rightFields);
      return;
    }
    
    for (String field : leftFields) {
      // nocommit cutover to per-segment comparison
      NumericDocValues leftNorms = MultiSimpleDocValues.simpleNormValues(leftReader, field);
      NumericDocValues rightNorms = MultiSimpleDocValues.simpleNormValues(rightReader, field);
      if (leftNorms != null && rightNorms != null) {
        assertDocValues(leftReader.maxDoc(), leftNorms, rightNorms);
      } else {
        assertNull(info, leftNorms);
        assertNull(info, rightNorms);
      }
    }
  }
  
  /** 
   * checks that stored fields of all documents are the same 
   */
  public void assertStoredFields(IndexReader leftReader, IndexReader rightReader) throws Exception {
    assert leftReader.maxDoc() == rightReader.maxDoc();
    for (int i = 0; i < leftReader.maxDoc(); i++) {
      StoredDocument leftDoc = leftReader.document(i);
      StoredDocument rightDoc = rightReader.document(i);
      
      // TODO: I think this is bogus because we don't document what the order should be
      // from these iterators, etc. I think the codec/IndexReader should be free to order this stuff
      // in whatever way it wants (e.g. maybe it packs related fields together or something)
      // To fix this, we sort the fields in both documents by name, but
      // we still assume that all instances with same name are in order:
      Comparator<StorableField> comp = new Comparator<StorableField>() {
        @Override
        public int compare(StorableField arg0, StorableField arg1) {
          return arg0.name().compareTo(arg1.name());
        }        
      };
      Collections.sort(leftDoc.getFields(), comp);
      Collections.sort(rightDoc.getFields(), comp);

      Iterator<StorableField> leftIterator = leftDoc.iterator();
      Iterator<StorableField> rightIterator = rightDoc.iterator();
      while (leftIterator.hasNext()) {
        assertTrue(info, rightIterator.hasNext());
        assertStoredField(leftIterator.next(), rightIterator.next());
      }
      assertFalse(info, rightIterator.hasNext());
    }
  }
  
  /** 
   * checks that two stored fields are equivalent 
   */
  public void assertStoredField(StorableField leftField, StorableField rightField) {
    assertEquals(info, leftField.name(), rightField.name());
    assertEquals(info, leftField.binaryValue(), rightField.binaryValue());
    assertEquals(info, leftField.stringValue(), rightField.stringValue());
    assertEquals(info, leftField.numericValue(), rightField.numericValue());
    // TODO: should we check the FT at all?
  }
  
  /** 
   * checks that term vectors across all fields are equivalent 
   */
  public void assertTermVectors(IndexReader leftReader, IndexReader rightReader) throws Exception {
    assert leftReader.maxDoc() == rightReader.maxDoc();
    for (int i = 0; i < leftReader.maxDoc(); i++) {
      Fields leftFields = leftReader.getTermVectors(i);
      Fields rightFields = rightReader.getTermVectors(i);
      assertFields(leftFields, rightFields, rarely());
    }
  }

  private static Set<String> getDVFields(IndexReader reader) {
    Set<String> fields = new HashSet<String>();
    for(FieldInfo fi : MultiFields.getMergedFieldInfos(reader)) {
      if (fi.hasDocValues()) {
        fields.add(fi.name);
      }
    }

    return fields;
  }
  
  /**
   * checks that docvalues across all fields are equivalent
   */
  public void assertDocValues(IndexReader leftReader, IndexReader rightReader) throws Exception {
    Set<String> leftFields = getDVFields(leftReader);
    Set<String> rightFields = getDVFields(rightReader);
    assertEquals(info, leftFields, rightFields);

    for (String field : leftFields) {

      {
        NumericDocValues leftValues = MultiSimpleDocValues.simpleNumericValues(leftReader, field);
        NumericDocValues rightValues = MultiSimpleDocValues.simpleNumericValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          assertDocValues(leftReader.maxDoc(), leftValues, rightValues);
        } else {
          assertNull(leftValues);
          assertNull(rightValues);
        }
      }

      {
        BinaryDocValues leftValues = MultiSimpleDocValues.simpleBinaryValues(leftReader, field);
        BinaryDocValues rightValues = MultiSimpleDocValues.simpleBinaryValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          BytesRef scratchLeft = new BytesRef();
          BytesRef scratchRight = new BytesRef();
          for(int docID=0;docID<leftReader.maxDoc();docID++) {
            leftValues.get(docID, scratchLeft);
            rightValues.get(docID, scratchRight);
            assertEquals(scratchLeft, scratchRight);
          }
        } else {
          assertNull(leftValues);
          assertNull(rightValues);
        }
      }
    }
  }
  
  public void assertDocValues(int num, NumericDocValues leftDocValues, NumericDocValues rightDocValues) throws Exception {
    assertNotNull(info, leftDocValues);
    assertNotNull(info, rightDocValues);
    for(int docID=0;docID<num;docID++) {
      assertEquals(leftDocValues.get(docID),
                   rightDocValues.get(docID));
    }
  }
  
  // TODO: this is kinda stupid, we don't delete documents in the test.
  public void assertDeletedDocs(IndexReader leftReader, IndexReader rightReader) throws Exception {
    assert leftReader.numDeletedDocs() == rightReader.numDeletedDocs();
    Bits leftBits = MultiFields.getLiveDocs(leftReader);
    Bits rightBits = MultiFields.getLiveDocs(rightReader);
    
    if (leftBits == null || rightBits == null) {
      assertNull(info, leftBits);
      assertNull(info, rightBits);
      return;
    }
    
    assert leftReader.maxDoc() == rightReader.maxDoc();
    assertEquals(info, leftBits.length(), rightBits.length());
    for (int i = 0; i < leftReader.maxDoc(); i++) {
      assertEquals(info, leftBits.get(i), rightBits.get(i));
    }
  }
  
  public void assertFieldInfos(IndexReader leftReader, IndexReader rightReader) throws Exception {
    FieldInfos leftInfos = MultiFields.getMergedFieldInfos(leftReader);
    FieldInfos rightInfos = MultiFields.getMergedFieldInfos(rightReader);
    
    // TODO: would be great to verify more than just the names of the fields!
    TreeSet<String> left = new TreeSet<String>();
    TreeSet<String> right = new TreeSet<String>();
    
    for (FieldInfo fi : leftInfos) {
      left.add(fi.name);
    }
    
    for (FieldInfo fi : rightInfos) {
      right.add(fi.name);
    }
    
    assertEquals(info, left, right);
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
