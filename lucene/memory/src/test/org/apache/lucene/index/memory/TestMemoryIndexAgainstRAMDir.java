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
package org.apache.lucene.index.memory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.RecyclingByteBlockAllocator;
import org.apache.lucene.util.TestUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Verifies that Lucene MemoryIndex and RAMDirectory have the same behaviour,
 * returning the same results for queries on some randomish indexes.
 */
public class TestMemoryIndexAgainstRAMDir extends BaseTokenStreamTestCase {
  private Set<String> queries = new HashSet<>();
  
  public static final int ITERATIONS = 100 * RANDOM_MULTIPLIER;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    queries.addAll(readQueries("testqueries.txt"));
    queries.addAll(readQueries("testqueries2.txt"));
  }
  
  /**
   * read a set of queries from a resource file
   */
  private Set<String> readQueries(String resource) throws IOException {
    Set<String> queries = new HashSet<>();
    InputStream stream = getClass().getResourceAsStream(resource);
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    String line = null;
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.length() > 0 && !line.startsWith("#") && !line.startsWith("//")) {
        queries.add(line);
      }
    }
    return queries;
  }
  
  
  /**
   * runs random tests, up to ITERATIONS times.
   */
  public void testRandomQueries() throws Exception {
    MemoryIndex index = randomMemoryIndex();
    for (int i = 0; i < ITERATIONS; i++) {
      assertAgainstRAMDirectory(index);
    }
  }
  
  /**
   * Build a randomish document for both RAMDirectory and MemoryIndex,
   * and run all the queries against it.
   */
  public void assertAgainstRAMDirectory(MemoryIndex memory) throws Exception {
    memory.reset();
    StringBuilder fooField = new StringBuilder();
    StringBuilder termField = new StringBuilder();
 
    // add up to 250 terms to field "foo"
    final int numFooTerms = random().nextInt(250 * RANDOM_MULTIPLIER);
    for (int i = 0; i < numFooTerms; i++) {
      fooField.append(" ");
      fooField.append(randomTerm());
    }

    // add up to 250 terms to field "term"
    final int numTermTerms = random().nextInt(250 * RANDOM_MULTIPLIER);
    for (int i = 0; i < numTermTerms; i++) {
      termField.append(" ");
      termField.append(randomTerm());
    }
    
    Directory ramdir = new RAMDirectory();
    Analyzer analyzer = randomAnalyzer();
    IndexWriter writer = new IndexWriter(ramdir,
                                         new IndexWriterConfig(analyzer).setCodec(
                                             TestUtil.alwaysPostingsFormat(TestUtil.getDefaultPostingsFormat())));
    Document doc = new Document();
    Field field1 = newTextField("foo", fooField.toString(), Field.Store.NO);
    Field field2 = newTextField("term", termField.toString(), Field.Store.NO);
    doc.add(field1);
    doc.add(field2);
    writer.addDocument(doc);
    writer.close();
    
    memory.addField("foo", fooField.toString(), analyzer);
    memory.addField("term", termField.toString(), analyzer);
    
    LeafReader reader = (LeafReader) memory.createSearcher().getIndexReader();
    TestUtil.checkReader(reader);
    DirectoryReader competitor = DirectoryReader.open(ramdir);
    duellReaders(competitor, reader);
    IOUtils.close(reader, competitor);
    assertAllQueries(memory, ramdir, analyzer);  
    ramdir.close();    
  }

  private void duellReaders(CompositeReader other, LeafReader memIndexReader)
      throws IOException {
    LeafReader competitor = SlowCompositeReaderWrapper.wrap(other);
    Fields memFields = memIndexReader.fields();
    for (String field : competitor.fields()) {
      Terms memTerms = memFields.terms(field);
      Terms iwTerms = memIndexReader.terms(field);
      if (iwTerms == null) {
        assertNull(memTerms);
      } else {
        NumericDocValues normValues = competitor.getNormValues(field);
        NumericDocValues memNormValues = memIndexReader.getNormValues(field);
        if (normValues != null) {
          // mem idx always computes norms on the fly
          assertNotNull(memNormValues);
          assertEquals(normValues.get(0), memNormValues.get(0));
        }
          
        assertNotNull(memTerms);
        assertEquals(iwTerms.getDocCount(), memTerms.getDocCount());
        assertEquals(iwTerms.getSumDocFreq(), memTerms.getSumDocFreq());
        assertEquals(iwTerms.getSumTotalTermFreq(), memTerms.getSumTotalTermFreq());
        TermsEnum iwTermsIter = iwTerms.iterator();
        TermsEnum memTermsIter = memTerms.iterator();
        if (iwTerms.hasPositions()) {
          final boolean offsets = iwTerms.hasOffsets() && memTerms.hasOffsets();
         
          while(iwTermsIter.next() != null) {
            assertNotNull(memTermsIter.next());
            assertEquals(iwTermsIter.term(), memTermsIter.term());
            PostingsEnum iwDocsAndPos = iwTermsIter.postings(null, PostingsEnum.ALL);
            PostingsEnum memDocsAndPos = memTermsIter.postings(null, PostingsEnum.ALL);
            while(iwDocsAndPos.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
              assertEquals(iwDocsAndPos.docID(), memDocsAndPos.nextDoc());
              assertEquals(iwDocsAndPos.freq(), memDocsAndPos.freq());
              for (int i = 0; i < iwDocsAndPos.freq(); i++) {
                assertEquals("term: " + iwTermsIter.term().utf8ToString(), iwDocsAndPos.nextPosition(), memDocsAndPos.nextPosition());
                if (offsets) {
                  assertEquals(iwDocsAndPos.startOffset(), memDocsAndPos.startOffset());
                  assertEquals(iwDocsAndPos.endOffset(), memDocsAndPos.endOffset());
                }

                if (iwTerms.hasPayloads()) {
                  assertEquals(iwDocsAndPos.getPayload(), memDocsAndPos.getPayload());
                }
              }
              
            }
            
          }
        } else {
          while(iwTermsIter.next() != null) {
            assertEquals(iwTermsIter.term(), memTermsIter.term());
            PostingsEnum iwDocsAndPos = iwTermsIter.postings(null);
            PostingsEnum memDocsAndPos = memTermsIter.postings(null);
            while(iwDocsAndPos.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
              assertEquals(iwDocsAndPos.docID(), memDocsAndPos.nextDoc());
              assertEquals(iwDocsAndPos.freq(), memDocsAndPos.freq());
            }
          }
        }
      }
      
    }
  }
  
  /**
   * Run all queries against both the RAMDirectory and MemoryIndex, ensuring they are the same.
   */
  public void assertAllQueries(MemoryIndex memory, Directory ramdir, Analyzer analyzer) throws Exception {
    IndexReader reader = DirectoryReader.open(ramdir);
    IndexSearcher ram = newSearcher(reader);
    IndexSearcher mem = memory.createSearcher();
    QueryParser qp = new QueryParser("foo", analyzer);
    for (String query : queries) {
      TopDocs ramDocs = ram.search(qp.parse(query), 1);
      TopDocs memDocs = mem.search(qp.parse(query), 1);
      assertEquals(query, ramDocs.totalHits, memDocs.totalHits);
    }
    reader.close();
  }
  
  /**
   * Return a random analyzer (Simple, Stop, Standard) to analyze the terms.
   */
  private Analyzer randomAnalyzer() {
    switch(random().nextInt(4)) {
      case 0: return new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
      case 1: return new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET);
      case 2: return new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer();
          return new TokenStreamComponents(tokenizer, new CrazyTokenFilter(tokenizer));
        }
      };
      default: return new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    }
  }
  
  // a tokenfilter that makes all terms starting with 't' empty strings
  static final class CrazyTokenFilter extends TokenFilter {
    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    
    CrazyTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (termAtt.length() > 0 && termAtt.buffer()[0] == 't') {
          termAtt.setLength(0);
        }
        return true;
      } else {
        return false;
      }
    }
  };
  
  /**
   * Some terms to be indexed, in addition to random words. 
   * These terms are commonly used in the queries. 
   */
  private static final String[] TEST_TERMS = {"term", "Term", "tErm", "TERM",
      "telm", "stop", "drop", "roll", "phrase", "a", "c", "bar", "blar",
      "gack", "weltbank", "worlbank", "hello", "on", "the", "apache", "Apache",
      "copyright", "Copyright"};
  
  
  /**
   * half of the time, returns a random term from TEST_TERMS.
   * the other half of the time, returns a random unicode string.
   */
  private String randomTerm() {
    if (random().nextBoolean()) {
      // return a random TEST_TERM
      return TEST_TERMS[random().nextInt(TEST_TERMS.length)];
    } else {
      // return a random unicode term
      return TestUtil.randomUnicodeString(random());
    }
  }
  
  public void testDocsEnumStart() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    MemoryIndex memory = new MemoryIndex(random().nextBoolean(), false, random().nextInt(50) * 1024 * 1024);
    memory.addField("foo", "bar", analyzer);
    LeafReader reader = (LeafReader) memory.createSearcher().getIndexReader();
    TestUtil.checkReader(reader);
    PostingsEnum disi = TestUtil.docs(random(), reader, "foo", new BytesRef("bar"), null, PostingsEnum.NONE);
    int docid = disi.docID();
    assertEquals(-1, docid);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    
    // now reuse and check again
    TermsEnum te = reader.terms("foo").iterator();
    assertTrue(te.seekExact(new BytesRef("bar")));
    disi = te.postings(disi, PostingsEnum.NONE);
    docid = disi.docID();
    assertEquals(-1, docid);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    reader.close();
  }
  
  private Allocator randomByteBlockAllocator() {
    if (random().nextBoolean()) {
      return new RecyclingByteBlockAllocator();
    } else {
      return new ByteBlockPool.DirectAllocator();
    }
  }

  private MemoryIndex randomMemoryIndex() {
    return new MemoryIndex(random().nextBoolean(), random().nextBoolean(), random().nextInt(50) * 1024 * 1024);
  }

  public void testDocsAndPositionsEnumStart() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    int numIters = atLeast(3);
    MemoryIndex memory = new MemoryIndex(true, false, random().nextInt(50) * 1024 * 1024);
    for (int i = 0; i < numIters; i++) { // check reuse
      memory.addField("foo", "bar", analyzer);
      LeafReader reader = (LeafReader) memory.createSearcher().getIndexReader();
      TestUtil.checkReader(reader);
      assertEquals(1, reader.terms("foo").getSumTotalTermFreq());
      PostingsEnum disi = reader.postings(new Term("foo", "bar"), PostingsEnum.ALL);
      int docid = disi.docID();
      assertEquals(-1, docid);
      assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(0, disi.nextPosition());
      assertEquals(0, disi.startOffset());
      assertEquals(3, disi.endOffset());
      
      // now reuse and check again
      TermsEnum te = reader.terms("foo").iterator();
      assertTrue(te.seekExact(new BytesRef("bar")));
      disi = te.postings(disi);
      docid = disi.docID();
      assertEquals(-1, docid);
      assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      reader.close();
      memory.reset();
    }
  }

  // LUCENE-3831
  public void testNullPointerException() throws IOException {
    RegexpQuery regex = new RegexpQuery(new Term("field", "worl."));
    SpanQuery wrappedquery = new SpanMultiTermQueryWrapper<>(regex);
        
    MemoryIndex mindex = randomMemoryIndex();
    mindex.addField("field", new MockAnalyzer(random()).tokenStream("field", "hello there"));

    // This throws an NPE
    assertEquals(0, mindex.search(wrappedquery), 0.00001f);
    TestUtil.checkReader(mindex.createSearcher().getIndexReader());
  }
    
  // LUCENE-3831
  public void testPassesIfWrapped() throws IOException {
    RegexpQuery regex = new RegexpQuery(new Term("field", "worl."));
    SpanQuery wrappedquery = new SpanOrQuery(new SpanMultiTermQueryWrapper<>(regex));

    MemoryIndex mindex = randomMemoryIndex();
    mindex.addField("field", new MockAnalyzer(random()).tokenStream("field", "hello there"));

    // This passes though
    assertEquals(0, mindex.search(wrappedquery), 0.00001f);
    TestUtil.checkReader(mindex.createSearcher().getIndexReader());
  }
  
  public void testSameFieldAddedMultipleTimes() throws IOException {
    MemoryIndex mindex = randomMemoryIndex();
    MockAnalyzer mockAnalyzer = new MockAnalyzer(random());
    mindex.addField("field", "the quick brown fox", mockAnalyzer);
    mindex.addField("field", "jumps over the", mockAnalyzer);
    LeafReader reader = (LeafReader) mindex.createSearcher().getIndexReader();
    TestUtil.checkReader(reader);
    assertEquals(7, reader.terms("field").getSumTotalTermFreq());
    PhraseQuery query = new PhraseQuery("field", "fox", "jumps");
    assertTrue(mindex.search(query) > 0.1);
    mindex.reset();
    mockAnalyzer.setPositionIncrementGap(1 + random().nextInt(10));
    mindex.addField("field", "the quick brown fox", mockAnalyzer);
    mindex.addField("field", "jumps over the", mockAnalyzer);
    assertEquals(0, mindex.search(query), 0.00001f);
    query = new PhraseQuery(10, "field", "fox", "jumps");
    assertTrue("posGap" + mockAnalyzer.getPositionIncrementGap("field") , mindex.search(query) > 0.0001);
    TestUtil.checkReader(mindex.createSearcher().getIndexReader());
  }
  
  public void testNonExistentField() throws IOException {
    MemoryIndex mindex = randomMemoryIndex();
    MockAnalyzer mockAnalyzer = new MockAnalyzer(random());
    mindex.addField("field", "the quick brown fox", mockAnalyzer);
    LeafReader reader = (LeafReader) mindex.createSearcher().getIndexReader();
    TestUtil.checkReader(reader);
    assertNull(reader.getNumericDocValues("not-in-index"));
    assertNull(reader.getNormValues("not-in-index"));
    assertNull(reader.postings(new Term("not-in-index", "foo")));
    assertNull(reader.postings(new Term("not-in-index", "foo"), PostingsEnum.ALL));
    assertNull(reader.terms("not-in-index"));
  }

  public void testDuellMemIndex() throws IOException {
    LineFileDocs lineFileDocs = new LineFileDocs(random());
    int numDocs = atLeast(10);
    MemoryIndex memory = randomMemoryIndex();
    for (int i = 0; i < numDocs; i++) {
      Directory dir = newDirectory();
      MockAnalyzer mockAnalyzer = new MockAnalyzer(random());
      mockAnalyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), mockAnalyzer));
      Document nextDoc = lineFileDocs.nextDoc();
      Document doc = new Document();
      for (IndexableField field : nextDoc.getFields()) {
        if (field.fieldType().indexOptions() != IndexOptions.NONE) {
          doc.add(field);
          if (random().nextInt(3) == 0) {
            doc.add(field);  // randomly add the same field twice
          }
        }
      }
      
      writer.addDocument(doc);
      writer.close();
      for (IndexableField field : doc.getFields()) {
        memory.addField(field.name(), ((Field)field).stringValue(), mockAnalyzer);  
      }
      DirectoryReader competitor = DirectoryReader.open(dir);
      LeafReader memIndexReader= (LeafReader) memory.createSearcher().getIndexReader();
      TestUtil.checkReader(memIndexReader);
      duellReaders(competitor, memIndexReader);
      IOUtils.close(competitor, memIndexReader);
      memory.reset();
      dir.close();
    }
    lineFileDocs.close();
  }
  
  // LUCENE-4880
  public void testEmptyString() throws IOException {
    MemoryIndex memory = new MemoryIndex();
    memory.addField("foo", new CannedTokenStream(new Token("", 0, 5)));
    IndexSearcher searcher = memory.createSearcher();
    TopDocs docs = searcher.search(new TermQuery(new Term("foo", "")), 10);
    assertEquals(1, docs.totalHits);
    TestUtil.checkReader(searcher.getIndexReader());
  }

  public void testDuelMemoryIndexCoreDirectoryWithArrayField() throws Exception {

    final String field_name = "text";
    MockAnalyzer mockAnalyzer = new MockAnalyzer(random());
    if (random().nextBoolean()) {
      mockAnalyzer.setOffsetGap(random().nextInt(100));
    }
    //index into a random directory
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPayloads(false);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();

    Document doc = new Document();
    doc.add(new Field(field_name, "la la", type));
    doc.add(new Field(field_name, "foo bar foo bar foo", type));

    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), mockAnalyzer));
    writer.updateDocument(new Term("id", "1"), doc);
    writer.commit();
    writer.close();
    DirectoryReader reader = DirectoryReader.open(dir);

    //Index document in Memory index
    MemoryIndex memIndex = new MemoryIndex(true);
    memIndex.addField(field_name, "la la", mockAnalyzer);
    memIndex.addField(field_name, "foo bar foo bar foo", mockAnalyzer);

    //compare term vectors
    Terms ramTv = reader.getTermVector(0, field_name);
    IndexReader memIndexReader = memIndex.createSearcher().getIndexReader();
    TestUtil.checkReader(memIndexReader);
    Terms memTv = memIndexReader.getTermVector(0, field_name);

    compareTermVectors(ramTv, memTv, field_name);
    memIndexReader.close();
    reader.close();
    dir.close();

  }

  protected void compareTermVectors(Terms terms, Terms memTerms, String field_name) throws IOException {

    TermsEnum termEnum = terms.iterator();
    TermsEnum memTermEnum = memTerms.iterator();

    while (termEnum.next() != null) {
      assertNotNull(memTermEnum.next());
      assertThat(termEnum.totalTermFreq(), equalTo(memTermEnum.totalTermFreq()));

      PostingsEnum docsPosEnum = termEnum.postings(null, PostingsEnum.POSITIONS);
      PostingsEnum memDocsPosEnum = memTermEnum.postings(null, PostingsEnum.POSITIONS);
      String currentTerm = termEnum.term().utf8ToString();

      assertThat("Token mismatch for field: " + field_name, currentTerm, equalTo(memTermEnum.term().utf8ToString()));

      docsPosEnum.nextDoc();
      memDocsPosEnum.nextDoc();

      int freq = docsPosEnum.freq();
      assertThat(freq, equalTo(memDocsPosEnum.freq()));
      for (int i = 0; i < freq; i++) {
        String failDesc = " (field:" + field_name + " term:" + currentTerm + ")";
        int memPos = memDocsPosEnum.nextPosition();
        int pos = docsPosEnum.nextPosition();
        assertThat("Position test failed" + failDesc, memPos, equalTo(pos));
        assertThat("Start offset test failed" + failDesc, memDocsPosEnum.startOffset(), equalTo(docsPosEnum.startOffset()));
        assertThat("End offset test failed" + failDesc, memDocsPosEnum.endOffset(), equalTo(docsPosEnum.endOffset()));
        assertThat("Missing payload test failed" + failDesc, docsPosEnum.getPayload(), equalTo(docsPosEnum.getPayload()));
      }
    }
    assertNull("Still some tokens not processed", memTermEnum.next());
  }
}
