package org.apache.lucene.index.memory;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util._TestUtil;

/**
 * Verifies that Lucene MemoryIndex and RAMDirectory have the same behaviour,
 * returning the same results for queries on some randomish indexes.
 */
public class MemoryIndexTest extends BaseTokenStreamTestCase {
  private Set<String> queries = new HashSet<String>();
  
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
    Set<String> queries = new HashSet<String>();
    InputStream stream = getClass().getResourceAsStream(resource);
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
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
    for (int i = 0; i < ITERATIONS; i++)
      assertAgainstRAMDirectory();
  }

  /**
   * Build a randomish document for both RAMDirectory and MemoryIndex,
   * and run all the queries against it.
   */
  public void assertAgainstRAMDirectory() throws Exception {
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
                                         new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer).setCodec(_TestUtil.alwaysPostingsFormat(new Lucene40PostingsFormat())));
    Document doc = new Document();
    Field field1 = newField("foo", fooField.toString(), TextField.TYPE_UNSTORED);
    Field field2 = newField("term", termField.toString(), TextField.TYPE_UNSTORED);
    doc.add(field1);
    doc.add(field2);
    writer.addDocument(doc);
    writer.close();
    
    MemoryIndex memory = new MemoryIndex();
    memory.addField("foo", fooField.toString(), analyzer);
    memory.addField("term", termField.toString(), analyzer);
    
    if (VERBOSE) {
      System.out.println("Random MemoryIndex:\n" + memory.toString());
      System.out.println("Same index as RAMDirectory: " +
        RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOf(ramdir)));
      System.out.println();
    } else {
      assertTrue(memory.getMemorySize() > 0L);
    }

    assertAllQueries(memory, ramdir, analyzer);  
    ramdir.close();    
  }
  
  /**
   * Run all queries against both the RAMDirectory and MemoryIndex, ensuring they are the same.
   */
  public void assertAllQueries(MemoryIndex memory, Directory ramdir, Analyzer analyzer) throws Exception {
    IndexReader reader = DirectoryReader.open(ramdir);
    IndexSearcher ram = new IndexSearcher(reader);
    IndexSearcher mem = memory.createSearcher();
    QueryParser qp = new QueryParser(TEST_VERSION_CURRENT, "foo", analyzer);
    for (String query : queries) {
      TopDocs ramDocs = ram.search(qp.parse(query), 1);
      TopDocs memDocs = mem.search(qp.parse(query), 1);
      assertEquals(ramDocs.totalHits, memDocs.totalHits);
    }
    reader.close();
  }
  
  /**
   * Return a random analyzer (Simple, Stop, Standard) to analyze the terms.
   */
  private Analyzer randomAnalyzer() {
    switch(random().nextInt(3)) {
      case 0: return new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
      case 1: return new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true);
      default: return new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    }
  }
  
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
      return _TestUtil.randomUnicodeString(random());
    }
  }
  
  public void testDocsEnumStart() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    MemoryIndex memory = new MemoryIndex();
    memory.addField("foo", "bar", analyzer);
    AtomicReader reader = (AtomicReader) memory.createSearcher().getIndexReader();
    DocsEnum disi = _TestUtil.docs(random(), reader, "foo", new BytesRef("bar"), null, null, false);
    int docid = disi.docID();
    assertTrue(docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    
    // now reuse and check again
    TermsEnum te = reader.terms("foo").iterator(null);
    assertTrue(te.seekExact(new BytesRef("bar"), true));
    disi = te.docs(null, disi, false);
    docid = disi.docID();
    assertTrue(docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    reader.close();
  }
  
  public void testDocsAndPositionsEnumStart() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    MemoryIndex memory = new MemoryIndex(true);
    memory.addField("foo", "bar", analyzer);
    AtomicReader reader = (AtomicReader) memory.createSearcher().getIndexReader();
    DocsAndPositionsEnum disi = reader.termPositionsEnum(null, "foo", new BytesRef("bar"), false);
    int docid = disi.docID();
    assertTrue(docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, disi.nextPosition());
    assertEquals(0, disi.startOffset());
    assertEquals(3, disi.endOffset());
    
    // now reuse and check again
    TermsEnum te = reader.terms("foo").iterator(null);
    assertTrue(te.seekExact(new BytesRef("bar"), true));
    disi = te.docsAndPositions(null, disi, false);
    docid = disi.docID();
    assertTrue(docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    reader.close();
  }

  // LUCENE-3831
  public void testNullPointerException() throws IOException {
    RegexpQuery regex = new RegexpQuery(new Term("field", "worl."));
    SpanQuery wrappedquery = new SpanMultiTermQueryWrapper<RegexpQuery>(regex);
        
    MemoryIndex mindex = new MemoryIndex();
    mindex.addField("field", new MockAnalyzer(random()).tokenStream("field", new StringReader("hello there")));

    // This throws an NPE
    assertEquals(0, mindex.search(wrappedquery), 0.00001f);
  }
    
  // LUCENE-3831
  public void testPassesIfWrapped() throws IOException {
    RegexpQuery regex = new RegexpQuery(new Term("field", "worl."));
    SpanQuery wrappedquery = new SpanOrQuery(new SpanMultiTermQueryWrapper<RegexpQuery>(regex));

    MemoryIndex mindex = new MemoryIndex();
    mindex.addField("field", new MockAnalyzer(random()).tokenStream("field", new StringReader("hello there")));

    // This passes though
    assertEquals(0, mindex.search(wrappedquery), 0.00001f);
  }
}
