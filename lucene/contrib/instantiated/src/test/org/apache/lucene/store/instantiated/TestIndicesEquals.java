package org.apache.lucene.store.instantiated;
/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiNorms;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;

/**
 * Asserts equality of content and behaviour of two index readers.
 */
public class TestIndicesEquals extends LuceneTestCase {

//  public void test2() throws Exception {
//    FSDirectory fsdir = FSDirectory.open(new File("/tmp/fatcorpus"));
//    IndexReader ir = IndexReader.open(fsdir, false);
//    InstantiatedIndex ii = new InstantiatedIndex(ir);
//    ir.close();
//    testEquals(fsdir, ii);
//  }


  public void testLoadIndexReader() throws Exception {
    Directory dir = newDirectory();

    // create dir data
    IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    
    for (int i = 0; i < 20; i++) {
      Document document = new Document();
      assembleDocument(document, i);
      indexWriter.addDocument(document);
    }
    indexWriter.close();

    // test load ii from index reader
    IndexReader ir = IndexReader.open(dir, false);
    InstantiatedIndex ii = new InstantiatedIndex(ir);
    ir.close();

    testEqualBehaviour(dir, ii);
    dir.close();
  }


  public void testInstantiatedIndexWriter() throws Exception {

    Directory dir = newDirectory();
    InstantiatedIndex ii = new InstantiatedIndex();
    
    // we need to pass the "same" random to both, so they surely index the same payload data.
    long seed = random.nextLong();
    
    // create dir data
    IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig(
                                                                        TEST_VERSION_CURRENT, new MockAnalyzer(new Random(seed))).setMergePolicy(newLogMergePolicy()));
    indexWriter.setInfoStream(VERBOSE ? System.out : null);
    if (VERBOSE) {
      System.out.println("TEST: make test index");
    }
    for (int i = 0; i < 500; i++) {
      Document document = new Document();
      assembleDocument(document, i);
      indexWriter.addDocument(document);
    }
    indexWriter.close();

    // test ii writer
    InstantiatedIndexWriter instantiatedIndexWriter = ii.indexWriterFactory(new MockAnalyzer(new Random(seed)), true);
    for (int i = 0; i < 500; i++) {
      Document document = new Document();
      assembleDocument(document, i);
      instantiatedIndexWriter.addDocument(document);
    }
    instantiatedIndexWriter.close();


    testEqualBehaviour(dir, ii);

    dir.close();

  }


  private void testTermDocsSomeMore(Directory aprioriIndex, InstantiatedIndex testIndex) throws Exception {

    IndexReader aprioriReader = IndexReader.open(aprioriIndex, false);
    IndexReader testReader = testIndex.indexReaderFactory();

    // test seek

    Term t = new Term("c", "danny");
    TermsEnum aprioriTermEnum = MultiFields.getTerms(aprioriReader, t.field()).iterator();
    aprioriTermEnum.seek(new BytesRef(t.text()));
    TermsEnum testTermEnum = MultiFields.getTerms(testReader, t.field()).iterator();
    testTermEnum.seek(new BytesRef(t.text()));
    assertEquals(aprioriTermEnum.term(), testTermEnum.term());

    DocsEnum aprioriTermDocs = aprioriTermEnum.docs(MultiFields.getDeletedDocs(aprioriReader), null);
    DocsEnum testTermDocs = testTermEnum.docs(MultiFields.getDeletedDocs(testReader), null);

    assertEquals(aprioriTermDocs.nextDoc(), testTermDocs.nextDoc());
    assertEquals(aprioriTermDocs.freq(), testTermDocs.freq());

    if (aprioriTermDocs.advance(4) != DocsEnum.NO_MORE_DOCS) {
      assertTrue(testTermDocs.advance(4) != DocsEnum.NO_MORE_DOCS);
      assertEquals(aprioriTermDocs.freq(), testTermDocs.freq());
      assertEquals(aprioriTermDocs.docID(), testTermDocs.docID());
    } else {
      assertEquals(DocsEnum.NO_MORE_DOCS, testTermDocs.advance(4));
    }

    if (aprioriTermDocs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
      assertTrue(testTermDocs.nextDoc() != DocsEnum.NO_MORE_DOCS);
      assertEquals(aprioriTermDocs.freq(), testTermDocs.freq());
      assertEquals(aprioriTermDocs.docID(), testTermDocs.docID());
    } else {
      assertEquals(DocsEnum.NO_MORE_DOCS, testTermDocs.nextDoc());
    }


    // beyond this point all next and skipto will return false

    if (aprioriTermDocs.advance(100) != DocsEnum.NO_MORE_DOCS) {
      assertTrue(testTermDocs.advance(100) != DocsEnum.NO_MORE_DOCS);
      assertEquals(aprioriTermDocs.freq(), testTermDocs.freq());
      assertEquals(aprioriTermDocs.docID(), testTermDocs.docID());
    } else {
      assertEquals(DocsEnum.NO_MORE_DOCS, testTermDocs.advance(100));
    }

    // start using the API the way one is supposed to use it

    t = new Term("", "");
    FieldsEnum apFieldsEnum = MultiFields.getFields(aprioriReader).iterator();
    String apFirstField = apFieldsEnum.next();

    FieldsEnum testFieldsEnum = MultiFields.getFields(testReader).iterator();
    String testFirstField = testFieldsEnum.next();
    assertEquals(apFirstField, testFirstField);

    aprioriTermEnum = apFieldsEnum.terms();
    testTermEnum = testFieldsEnum.terms();
    
    assertEquals(aprioriTermEnum.next(), testTermEnum.next());
    
    aprioriTermDocs = aprioriTermEnum.docs(MultiFields.getDeletedDocs(aprioriReader), aprioriTermDocs);
    testTermDocs = testTermEnum.docs(MultiFields.getDeletedDocs(testReader), testTermDocs);

    while (aprioriTermDocs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
      assertTrue(testTermDocs.nextDoc() != DocsEnum.NO_MORE_DOCS);
      assertEquals(aprioriTermDocs.freq(), testTermDocs.freq());
      assertEquals(aprioriTermDocs.docID(), testTermDocs.docID());
    }
    assertEquals(DocsEnum.NO_MORE_DOCS, testTermDocs.nextDoc());

    // clean up
    aprioriReader.close();
    testReader.close();

  }


  private void assembleDocument(Document document, int i) {
    document.add(new Field("a", i + " Do you really want to go and live in that house all winter?", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    if (i > 0) {
      document.add(new Field("b0", i + " All work and no play makes Jack a dull boy", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      document.add(new Field("b1", i + " All work and no play makes Jack a dull boy", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
      document.add(new Field("b2", i + " All work and no play makes Jack a dull boy", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.NO));
      document.add(new Field("b3", i + " All work and no play makes Jack a dull boy", Field.Store.YES, Field.Index.NO, Field.TermVector.NO));
      if (i > 1) {
        document.add(new Field("c", i + " Redrum redrum", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        if (i > 2) {
          document.add(new Field("d", i + " Hello Danny, come and play with us... forever and ever. and ever.", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
          if (i > 3) {
            Field f = new Field("e", i + " Heres Johnny!", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
            f.setOmitNorms(true);
            document.add(f);
            if (i > 4) {
              final List<Token> tokens = new ArrayList<Token>(2);
              Token t = createToken("the", 0, 2, "text");
              t.setPayload(new Payload(new byte[]{1, 2, 3}));
              tokens.add(t);
              t = createToken("end", 3, 5, "text");
              t.setPayload(new Payload(new byte[]{2}));
              tokens.add(t);
              tokens.add(createToken("fin", 7, 9));
              TokenStream ts = new TokenStream(Token.TOKEN_ATTRIBUTE_FACTORY) {
                final AttributeImpl reusableToken = (AttributeImpl) addAttribute(CharTermAttribute.class);
                Iterator<Token> it = tokens.iterator();
                
                @Override
                public final boolean incrementToken() throws IOException {
                  if (!it.hasNext()) {
                    return false;
                  }
                  clearAttributes();
                  it.next().copyTo(reusableToken);
                  return true;
                }

                @Override
                public void reset() throws IOException {
                  it = tokens.iterator();
                }
              };
              
              document.add(new Field("f", ts));
            }
          }
        }
      }
    }
  }


  /**
   * Asserts that the content of two index readers equal each other.
   *
   * @param aprioriIndex the index that is known to be correct
   * @param testIndex    the index that is supposed to equals the apriori index.
   * @throws Exception
   */
  protected void testEqualBehaviour(Directory aprioriIndex, InstantiatedIndex testIndex) throws Exception {


    testEquals(aprioriIndex,  testIndex);

    // delete a few documents
    IndexReader air = IndexReader.open(aprioriIndex, false);
    InstantiatedIndexReader tir = testIndex.indexReaderFactory();

    assertEquals(air.isCurrent(), tir.isCurrent());
    assertEquals(air.hasDeletions(), tir.hasDeletions());
    assertEquals(air.maxDoc(), tir.maxDoc());
    assertEquals(air.numDocs(), tir.numDocs());
    assertEquals(air.numDeletedDocs(), tir.numDeletedDocs());

    air.deleteDocument(3);
    tir.deleteDocument(3);

    assertEquals(air.isCurrent(), tir.isCurrent());
    assertEquals(air.hasDeletions(), tir.hasDeletions());
    assertEquals(air.maxDoc(), tir.maxDoc());
    assertEquals(air.numDocs(), tir.numDocs());
    assertEquals(air.numDeletedDocs(), tir.numDeletedDocs());

    air.deleteDocument(8);
    tir.deleteDocument(8);

    assertEquals(air.isCurrent(), tir.isCurrent());
    assertEquals(air.hasDeletions(), tir.hasDeletions());
    assertEquals(air.maxDoc(), tir.maxDoc());
    assertEquals(air.numDocs(), tir.numDocs());
    assertEquals(air.numDeletedDocs(), tir.numDeletedDocs());    

    // this (in 3.0) commits the deletions
    air.close();
    tir.close();

    air = IndexReader.open(aprioriIndex, false);
    tir = testIndex.indexReaderFactory();

    assertEquals(air.isCurrent(), tir.isCurrent());
    assertEquals(air.hasDeletions(), tir.hasDeletions());
    assertEquals(air.maxDoc(), tir.maxDoc());
    assertEquals(air.numDocs(), tir.numDocs());
    assertEquals(air.numDeletedDocs(), tir.numDeletedDocs());

    final Bits aDelDocs = MultiFields.getDeletedDocs(air);
    final Bits tDelDocs = MultiFields.getDeletedDocs(tir);
    assertTrue((aDelDocs != null && tDelDocs != null) || 
               (aDelDocs == null && tDelDocs == null));
    if (aDelDocs != null) {
      for (int d =0; d<air.maxDoc(); d++) {
        assertEquals(aDelDocs.get(d), tDelDocs.get(d));
      }
    }

    air.close();
    tir.close();


    // make sure they still equal
    testEquals(aprioriIndex,  testIndex);
  }

  protected void testEquals(Directory aprioriIndex, InstantiatedIndex testIndex) throws Exception {

    if (VERBOSE) {
      System.out.println("TEST: testEquals");
    }
    testTermDocsSomeMore(aprioriIndex, testIndex);

    IndexReader aprioriReader = IndexReader.open(aprioriIndex, false);
    IndexReader testReader = testIndex.indexReaderFactory();

    assertEquals(aprioriReader.numDocs(), testReader.numDocs());

    // assert field options
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.INDEXED), testReader.getFieldNames(IndexReader.FieldOption.INDEXED));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.INDEXED_NO_TERMVECTOR), testReader.getFieldNames(IndexReader.FieldOption.INDEXED_NO_TERMVECTOR));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.INDEXED_WITH_TERMVECTOR), testReader.getFieldNames(IndexReader.FieldOption.INDEXED_WITH_TERMVECTOR));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.STORES_PAYLOADS), testReader.getFieldNames(IndexReader.FieldOption.STORES_PAYLOADS));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR), testReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET), testReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION), testReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET), testReader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET));
    assertEquals(aprioriReader.getFieldNames(IndexReader.FieldOption.UNINDEXED), testReader.getFieldNames(IndexReader.FieldOption.UNINDEXED));

    for (Object field : aprioriReader.getFieldNames(IndexReader.FieldOption.ALL)) {

      // test norms as used by normal use

      byte[] aprioriNorms = MultiNorms.norms(aprioriReader, (String) field);
      byte[] testNorms = MultiNorms.norms(testReader, (String) field);

      if (aprioriNorms != null) {
        assertEquals(aprioriNorms.length, testNorms.length);

        for (int i = 0; i < aprioriNorms.length; i++) {
          assertEquals("norms does not equals for field " + field + " in document " + i, aprioriNorms[i], testNorms[i]);
        }
      }
    }

    final Bits apDelDocs = MultiFields.getDeletedDocs(aprioriReader);
    final Bits testDelDocs = MultiFields.getDeletedDocs(testReader);
    assertTrue((apDelDocs != null && testDelDocs != null) || 
               (apDelDocs == null && testDelDocs == null));
    if (apDelDocs != null) {
      for (int docIndex = 0; docIndex < aprioriReader.numDocs(); docIndex++) {
        assertEquals(apDelDocs.get(docIndex), testDelDocs.get(docIndex));
      }
    }

    // compare term enumeration stepping

    FieldsEnum aprioriFieldsEnum = MultiFields.getFields(aprioriReader).iterator();
    FieldsEnum testFieldsEnum = MultiFields.getFields(testReader).iterator();

    String aprioriField;
    while((aprioriField = aprioriFieldsEnum.next()) != null) {
      String testField = testFieldsEnum.next();
      if (VERBOSE) {
        System.out.println("TEST: verify field=" + testField);
      }
      assertEquals(aprioriField, testField);

      TermsEnum aprioriTermEnum = aprioriFieldsEnum.terms();
      TermsEnum testTermEnum = testFieldsEnum.terms();

      BytesRef aprioriText;
      while((aprioriText = aprioriTermEnum.next()) != null) {
        assertEquals(aprioriText, testTermEnum.next());
        if (VERBOSE) {
          System.out.println("TEST:   verify term=" + aprioriText.utf8ToString());
        }

        assertTrue(aprioriTermEnum.docFreq() == testTermEnum.docFreq());
        final long totalTermFreq = aprioriTermEnum.totalTermFreq();
        if (totalTermFreq != -1) {
          assertEquals(totalTermFreq, testTermEnum.totalTermFreq());
        }

        // compare termDocs seeking

        DocsEnum aprioriTermDocs = aprioriTermEnum.docs(MultiFields.getDeletedDocs(aprioriReader), null);
        DocsEnum testTermDocs = testTermEnum.docs(MultiFields.getDeletedDocs(testReader), null);
        
        while (aprioriTermDocs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
          assertTrue(testTermDocs.advance(aprioriTermDocs.docID()) != DocsEnum.NO_MORE_DOCS);
          assertEquals(aprioriTermDocs.docID(), testTermDocs.docID());
        }
        
        // compare documents per term
        
        assertEquals(aprioriReader.docFreq(aprioriField, aprioriTermEnum.term()), testReader.docFreq(aprioriField, testTermEnum.term()));

        aprioriTermDocs = aprioriTermEnum.docs(MultiFields.getDeletedDocs(aprioriReader), aprioriTermDocs);
        testTermDocs = testTermEnum.docs(MultiFields.getDeletedDocs(testReader), testTermDocs);

        while (true) {
          if (aprioriTermDocs.nextDoc() == DocsEnum.NO_MORE_DOCS) {
            assertEquals(DocsEnum.NO_MORE_DOCS, testTermDocs.nextDoc());
            break;
          }
          if (VERBOSE) {
            System.out.println("TEST:     verify doc=" + aprioriTermDocs.docID());
          }

          assertTrue(testTermDocs.nextDoc() != DocsEnum.NO_MORE_DOCS);

          assertEquals(aprioriTermDocs.docID(), testTermDocs.docID());
          assertEquals(aprioriTermDocs.freq(), testTermDocs.freq());
        }

        // compare term positions

        DocsAndPositionsEnum aprioriTermPositions = aprioriTermEnum.docsAndPositions(MultiFields.getDeletedDocs(aprioriReader), null);
        DocsAndPositionsEnum testTermPositions = testTermEnum.docsAndPositions(MultiFields.getDeletedDocs(testReader), null);

        if (VERBOSE) {
          System.out.println("TEST: enum1=" + aprioriTermPositions + " enum2=" + testTermPositions);
        }
        if (aprioriTermPositions != null) {

          for (int docIndex = 0; docIndex < aprioriReader.maxDoc(); docIndex++) {
            boolean hasNext = aprioriTermPositions.nextDoc() != DocsEnum.NO_MORE_DOCS;
            if (hasNext) {
              assertTrue(testTermPositions.nextDoc() != DocsEnum.NO_MORE_DOCS);

              if (VERBOSE) {
                System.out.println("TEST:     verify doc=" + aprioriTermPositions.docID());
              }
              
              assertEquals(aprioriTermPositions.freq(), testTermPositions.freq());

              for (int termPositionIndex = 0; termPositionIndex < aprioriTermPositions.freq(); termPositionIndex++) {
                int aprioriPos = aprioriTermPositions.nextPosition();
                int testPos = testTermPositions.nextPosition();

                if (VERBOSE) {
                  System.out.println("TEST:       verify pos=" + aprioriPos);
                }

                assertEquals(aprioriPos, testPos);

                assertEquals(aprioriTermPositions.hasPayload(), testTermPositions.hasPayload());
                if (aprioriTermPositions.hasPayload()) {
                  BytesRef apPayload = aprioriTermPositions.getPayload();
                  BytesRef testPayload = testTermPositions.getPayload();
                  assertEquals(apPayload, testPayload);
                }
              }
            }
          }
        }
      }
      assertNull(testTermEnum.next());
    }
    assertNull(testFieldsEnum.next());

    // compare term vectors and position vectors

    for (int documentNumber = 0; documentNumber < aprioriReader.numDocs(); documentNumber++) {

      if (documentNumber > 0) {
        assertNotNull(aprioriReader.getTermFreqVector(documentNumber, "b0"));
        assertNull(aprioriReader.getTermFreqVector(documentNumber, "b1"));

        assertNotNull(testReader.getTermFreqVector(documentNumber, "b0"));
        assertNull(testReader.getTermFreqVector(documentNumber, "b1"));

      }

      TermFreqVector[] aprioriFreqVectors = aprioriReader.getTermFreqVectors(documentNumber);
      TermFreqVector[] testFreqVectors = testReader.getTermFreqVectors(documentNumber);

      if (aprioriFreqVectors != null && testFreqVectors != null) {

        Arrays.sort(aprioriFreqVectors, new Comparator<TermFreqVector>() {
          public int compare(TermFreqVector termFreqVector, TermFreqVector termFreqVector1) {
            return termFreqVector.getField().compareTo(termFreqVector1.getField());
          }
        });
        Arrays.sort(testFreqVectors, new Comparator<TermFreqVector>() {
          public int compare(TermFreqVector termFreqVector, TermFreqVector termFreqVector1) {
            return termFreqVector.getField().compareTo(termFreqVector1.getField());
          }
        });

        assertEquals("document " + documentNumber + " vectors does not match", aprioriFreqVectors.length, testFreqVectors.length);

        for (int freqVectorIndex = 0; freqVectorIndex < aprioriFreqVectors.length; freqVectorIndex++) {
          assertTrue(Arrays.equals(aprioriFreqVectors[freqVectorIndex].getTermFrequencies(), testFreqVectors[freqVectorIndex].getTermFrequencies()));
          assertTrue(Arrays.equals(aprioriFreqVectors[freqVectorIndex].getTerms(), testFreqVectors[freqVectorIndex].getTerms()));

          if (aprioriFreqVectors[freqVectorIndex] instanceof TermPositionVector) {
            TermPositionVector aprioriTermPositionVector = (TermPositionVector) aprioriFreqVectors[freqVectorIndex];
            TermPositionVector testTermPositionVector = (TermPositionVector) testFreqVectors[freqVectorIndex];

            for (int positionVectorIndex = 0; positionVectorIndex < aprioriFreqVectors[freqVectorIndex].getTerms().length; positionVectorIndex++)
            {
              if (aprioriTermPositionVector.getOffsets(positionVectorIndex) != null) {
                assertTrue(Arrays.equals(aprioriTermPositionVector.getOffsets(positionVectorIndex), testTermPositionVector.getOffsets(positionVectorIndex)));
              }

              if (aprioriTermPositionVector.getTermPositions(positionVectorIndex) != null) {
                assertTrue(Arrays.equals(aprioriTermPositionVector.getTermPositions(positionVectorIndex), testTermPositionVector.getTermPositions(positionVectorIndex)));
              }
            }
          }

        }
      }
    }

    aprioriReader.close();
    testReader.close();
  }

  private static Token createToken(String term, int start, int offset)
  {
    return new Token(term, start, offset);
  }

  private static Token createToken(String term, int start, int offset, String type)
  {
    return new Token(term, start, offset, type);
  }


}
