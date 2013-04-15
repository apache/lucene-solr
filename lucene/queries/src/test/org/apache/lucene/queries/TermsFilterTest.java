package org.apache.lucene.queries;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TermsFilterTest extends LuceneTestCase {

  public void testCachability() throws Exception {
    TermsFilter a = termsFilter(random().nextBoolean(), new Term("field1", "a"), new Term("field1", "b"));
    HashSet<Filter> cachedFilters = new HashSet<Filter>();
    cachedFilters.add(a);
    TermsFilter b = termsFilter(random().nextBoolean(), new Term("field1", "b"), new Term("field1", "a"));
    assertTrue("Must be cached", cachedFilters.contains(b));
    //duplicate term
    assertTrue("Must be cached", cachedFilters.contains(termsFilter(true, new Term("field1", "a"), new Term("field1", "a"), new Term("field1", "b"))));
    assertFalse("Must not be cached", cachedFilters.contains(termsFilter(random().nextBoolean(), new Term("field1", "a"), new Term("field1", "a"), new Term("field1", "b"),  new Term("field1", "v"))));
  }

  public void testMissingTerms() throws Exception {
    String fieldName = "field1";
    Directory rd = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), rd);
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int term = i * 10; //terms are units of 10;
      doc.add(newStringField(fieldName, "" + term, Field.Store.YES));
      w.addDocument(doc);
    }
    IndexReader reader = new SlowCompositeReaderWrapper(w.getReader());
    assertTrue(reader.getContext() instanceof AtomicReaderContext);
    AtomicReaderContext context = (AtomicReaderContext) reader.getContext();
    w.close();

    List<Term> terms = new ArrayList<Term>();
    terms.add(new Term(fieldName, "19"));
    FixedBitSet bits = (FixedBitSet) termsFilter(random().nextBoolean(), terms).getDocIdSet(context, context.reader().getLiveDocs());
    assertNull("Must match nothing", bits);

    terms.add(new Term(fieldName, "20"));
    bits = (FixedBitSet) termsFilter(random().nextBoolean(), terms).getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match 1", 1, bits.cardinality());

    terms.add(new Term(fieldName, "10"));
    bits = (FixedBitSet) termsFilter(random().nextBoolean(), terms).getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match 2", 2, bits.cardinality());

    terms.add(new Term(fieldName, "00"));
    bits = (FixedBitSet) termsFilter(random().nextBoolean(), terms).getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must match 2", 2, bits.cardinality());

    reader.close();
    rd.close();
  }
  
  public void testMissingField() throws Exception {
    String fieldName = "field1";
    Directory rd1 = newDirectory();
    RandomIndexWriter w1 = new RandomIndexWriter(random(), rd1);
    Document doc = new Document();
    doc.add(newStringField(fieldName, "content1", Field.Store.YES));
    w1.addDocument(doc);
    IndexReader reader1 = w1.getReader();
    w1.close();
    
    fieldName = "field2";
    Directory rd2 = newDirectory();
    RandomIndexWriter w2 = new RandomIndexWriter(random(), rd2);
    doc = new Document();
    doc.add(newStringField(fieldName, "content2", Field.Store.YES));
    w2.addDocument(doc);
    IndexReader reader2 = w2.getReader();
    w2.close();
    
    TermsFilter tf = new TermsFilter(new Term(fieldName, "content1"));
    MultiReader multi = new MultiReader(reader1, reader2);
    for (AtomicReaderContext context : multi.leaves()) {
      DocIdSet docIdSet = tf.getDocIdSet(context, context.reader().getLiveDocs());
      if (context.reader().docFreq(new Term(fieldName, "content1")) == 0) {
        assertNull(docIdSet);
      } else {
        FixedBitSet bits = (FixedBitSet) docIdSet;
        assertTrue("Must be >= 0", bits.cardinality() >= 0);      
      }
    }
    multi.close();
    reader1.close();
    reader2.close();
    rd1.close();
    rd2.close();
  }
  
  public void testFieldNotPresent() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int num = atLeast(3);
    int skip = random().nextInt(num);
    List<Term> terms = new ArrayList<Term>();
    for (int i = 0; i < num; i++) {
      terms.add(new Term("field" + i, "content1"));
      Document doc = new Document();
      if (skip == i) {
        continue;
      }
      doc.add(newStringField("field" + i, "content1", Field.Store.YES));
      w.addDocument(doc);  
    }
    
    w.forceMerge(1);
    IndexReader reader = w.getReader();
    w.close();
    assertEquals(1, reader.leaves().size());
    
    
    
    AtomicReaderContext context = reader.leaves().get(0);
    TermsFilter tf = new TermsFilter(terms);

    FixedBitSet bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals("Must be num fields - 1 since we skip only one field", num-1, bits.cardinality());  
    reader.close();
    dir.close();
  }
  
  public void testSkipField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int num = atLeast(10);
    Set<Term> terms = new HashSet<Term>();
    for (int i = 0; i < num; i++) {
      String field = "field" + random().nextInt(100);
      terms.add(new Term(field, "content1"));
      Document doc = new Document();
      doc.add(newStringField(field, "content1", Field.Store.YES));
      w.addDocument(doc);
    }
    int randomFields = random().nextInt(10);
    for (int i = 0; i < randomFields; i++) {
      while (true) {
        String field = "field" + random().nextInt(100);
        Term t = new Term(field, "content1");
        if (!terms.contains(t)) {
          terms.add(t);
          break;
        }
      }
    }
    w.forceMerge(1);
    IndexReader reader = w.getReader();
    w.close();
    assertEquals(1, reader.leaves().size());
    AtomicReaderContext context = reader.leaves().get(0);
    TermsFilter tf = new TermsFilter(new ArrayList<Term>(terms));

    FixedBitSet bits = (FixedBitSet) tf.getDocIdSet(context, context.reader().getLiveDocs());
    assertEquals(context.reader().numDocs(), bits.cardinality());  
    reader.close();
    dir.close();
  }
  
  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int num = atLeast(100);
    final boolean singleField = random().nextBoolean();
    List<Term> terms = new ArrayList<Term>();
    for (int i = 0; i < num; i++) {
      String field = "field" + (singleField ? "1" : random().nextInt(100));
      String string = _TestUtil.randomRealisticUnicodeString(random());
      terms.add(new Term(field, string));
      Document doc = new Document();
      doc.add(newStringField(field, string, Field.Store.YES));
      w.addDocument(doc);
    }
    IndexReader reader = w.getReader();
    w.close();
    
    IndexSearcher searcher = newSearcher(reader);
    
    int numQueries = atLeast(10);
    for (int i = 0; i < numQueries; i++) {
      Collections.shuffle(terms, random());
      int numTerms = 1 + random().nextInt(
          Math.min(BooleanQuery.getMaxClauseCount(), terms.size()));
      BooleanQuery bq = new BooleanQuery();
      for (int j = 0; j < numTerms; j++) {
        bq.add(new BooleanClause(new TermQuery(terms.get(j)), Occur.SHOULD));
      }
      TopDocs queryResult = searcher.search(new ConstantScoreQuery(bq), reader.maxDoc());
      
      MatchAllDocsQuery matchAll = new MatchAllDocsQuery();
      final TermsFilter filter = termsFilter(singleField, terms.subList(0, numTerms));;
      TopDocs filterResult = searcher.search(matchAll, filter, reader.maxDoc());
      assertEquals(filterResult.totalHits, queryResult.totalHits);
      ScoreDoc[] scoreDocs = filterResult.scoreDocs;
      for (int j = 0; j < scoreDocs.length; j++) {
        assertEquals(scoreDocs[j].doc, queryResult.scoreDocs[j].doc);
      }
    }
    
    reader.close();
    dir.close();
  }
  
  private TermsFilter termsFilter(boolean singleField, Term...terms) {
    return termsFilter(singleField, Arrays.asList(terms));
  }

  private TermsFilter termsFilter(boolean singleField, Collection<Term> termList) {
    if (!singleField) {
      return new TermsFilter(new ArrayList<Term>(termList));
    }
    final TermsFilter filter;
    List<BytesRef> bytes = new ArrayList<BytesRef>();
    String field = null;
    for (Term term : termList) {
        bytes.add(term.bytes());
        if (field != null) {
          assertEquals(term.field(), field);
        }
        field = term.field();
    }
    assertNotNull(field);
    filter = new TermsFilter(field, bytes);
    return filter;
  }
  
  public void testHashCodeAndEquals() {
    int num = atLeast(100);
    final boolean singleField = random().nextBoolean();
    List<Term> terms = new ArrayList<Term>();
    Set<Term> uniqueTerms = new HashSet<Term>();
    for (int i = 0; i < num; i++) {
      String field = "field" + (singleField ? "1" : random().nextInt(100));
      String string = _TestUtil.randomRealisticUnicodeString(random());
      terms.add(new Term(field, string));
      uniqueTerms.add(new Term(field, string));
      TermsFilter left = termsFilter(singleField ? random().nextBoolean() : false, uniqueTerms);
      Collections.shuffle(terms, random());
      TermsFilter right = termsFilter(singleField ? random().nextBoolean() : false, terms);
      assertEquals(right, left);
      assertEquals(right.hashCode(), left.hashCode());
      if (uniqueTerms.size() > 1) {
        List<Term> asList = new ArrayList<Term>(uniqueTerms);
        asList.remove(0);
        TermsFilter notEqual = termsFilter(singleField ? random().nextBoolean() : false, asList);
        assertFalse(left.equals(notEqual));
        assertFalse(right.equals(notEqual));
      }
    }
  }
  
  public void testNoTerms() {
    List<Term> emptyTerms = Collections.emptyList();
    List<BytesRef> emptyBytesRef = Collections.emptyList();
    try {
      new TermsFilter(emptyTerms);
      fail("must fail - no terms!");
    } catch (IllegalArgumentException e) {}
    
    try {
      new TermsFilter(emptyTerms.toArray(new Term[0]));
      fail("must fail - no terms!");
    } catch (IllegalArgumentException e) {}
    
    try {
      new TermsFilter(null, emptyBytesRef.toArray(new BytesRef[0]));
      fail("must fail - no terms!");
    } catch (IllegalArgumentException e) {}
    
    try {
      new TermsFilter(null, emptyBytesRef);
      fail("must fail - no terms!");
    } catch (IllegalArgumentException e) {}
  }

  public void testToString() {
    TermsFilter termsFilter = new TermsFilter(new Term("field1", "a"),
                                              new Term("field1", "b"),
                                              new Term("field1", "c"));
    assertEquals("field1:a field1:b field1:c", termsFilter.toString());
  }
}
