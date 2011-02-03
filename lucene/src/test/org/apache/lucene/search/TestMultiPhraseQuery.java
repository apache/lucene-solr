package org.apache.lucene.search;

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

import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation.IDFExplanation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * This class tests the MultiPhraseQuery class.
 * 
 * 
 */
public class TestMultiPhraseQuery extends LuceneTestCase {
  
  public void testPhrasePrefix() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    add("blueberry pie", writer);
    add("blueberry strudel", writer);
    add("blueberry pizza", writer);
    add("blueberry chewing gum", writer);
    add("bluebird pizza", writer);
    add("bluebird foobar pizza", writer);
    add("piccadilly circus", writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    
    // search for "blueberry pi*":
    MultiPhraseQuery query1 = new MultiPhraseQuery();
    // search for "strawberry pi*":
    MultiPhraseQuery query2 = new MultiPhraseQuery();
    query1.add(new Term("body", "blueberry"));
    query2.add(new Term("body", "strawberry"));

    LinkedList<Term> termsWithPrefix = new LinkedList<Term>();
    IndexReader ir = reader;

    // this TermEnum gives "piccadilly", "pie" and "pizza".
    String prefix = "pi";
    TermEnum te = ir.terms(new Term("body", prefix));
    do {
        if (te.term().text().startsWith(prefix))
        {
            termsWithPrefix.add(te.term());
        }
    } while (te.next());

    query1.add(termsWithPrefix.toArray(new Term[0]));
    assertEquals("body:\"blueberry (piccadilly pie pizza)\"", query1.toString());
    query2.add(termsWithPrefix.toArray(new Term[0]));
    assertEquals("body:\"strawberry (piccadilly pie pizza)\"", query2.toString());

    ScoreDoc[] result;
    result = searcher.search(query1, null, 1000).scoreDocs;
    assertEquals(2, result.length);
    result = searcher.search(query2, null, 1000).scoreDocs;
    assertEquals(0, result.length);

    // search for "blue* pizza":
    MultiPhraseQuery query3 = new MultiPhraseQuery();
    termsWithPrefix.clear();
    prefix = "blue";
    te = ir.terms(new Term("body", prefix));
    do {
        if (te.term().text().startsWith(prefix))
        {
            termsWithPrefix.add(te.term());
        }
    } while (te.next());
    query3.add(termsWithPrefix.toArray(new Term[0]));
    query3.add(new Term("body", "pizza"));

    result = searcher.search(query3, null, 1000).scoreDocs;
    assertEquals(2, result.length); // blueberry pizza, bluebird pizza
    assertEquals("body:\"(blueberry bluebird) pizza\"", query3.toString());

    // test slop:
    query3.setSlop(1);
    result = searcher.search(query3, null, 1000).scoreDocs;

    // just make sure no exc:
    searcher.explain(query3, 0);

    assertEquals(3, result.length); // blueberry pizza, bluebird pizza, bluebird foobar pizza

    MultiPhraseQuery query4 = new MultiPhraseQuery();
    try {
      query4.add(new Term("field1", "foo"));
      query4.add(new Term("field2", "foobar"));
      fail();
    } catch(IllegalArgumentException e) {
      // okay, all terms must belong to the same field
    }
    
    writer.close();
    searcher.close();
    reader.close();
    indexStore.close();
  }

  // LUCENE-2580
  public void testTall() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    add("blueberry chocolate pie", writer);
    add("blueberry chocolate tart", writer);
    IndexReader r = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(r);
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(new Term("body", "blueberry"));
    q.add(new Term("body", "chocolate"));
    q.add(new Term[] {new Term("body", "pie"), new Term("body", "tart")});
    assertEquals(2, searcher.search(q, 1).totalHits);
    searcher.close();
    r.close();
    indexStore.close();
  }
  
  private void add(String s, RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newField("body", s, Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
  }
  
  public void testBooleanQueryContainingSingleTermPrefixQuery()
      throws IOException {
    // this tests against bug 33161 (now fixed)
    // In order to cause the bug, the outer query must have more than one term
    // and all terms required.
    // The contained PhraseMultiQuery must contain exactly one term array.
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    add("blueberry pie", writer);
    add("blueberry chewing gum", writer);
    add("blue raspberry pie", writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    // This query will be equivalent to +body:pie +body:"blue*"
    BooleanQuery q = new BooleanQuery();
    q.add(new TermQuery(new Term("body", "pie")), BooleanClause.Occur.MUST);
    
    MultiPhraseQuery trouble = new MultiPhraseQuery();
    trouble.add(new Term[] {new Term("body", "blueberry"),
        new Term("body", "blue")});
    q.add(trouble, BooleanClause.Occur.MUST);
    
    // exception will be thrown here without fix
    ScoreDoc[] hits = searcher.search(q, null, 1000).scoreDocs;
    
    assertEquals("Wrong number of hits", 2, hits.length);
    
    // just make sure no exc:
    searcher.explain(q, 0);
    
    writer.close();
    searcher.close();
    reader.close();
    indexStore.close();
  }
  
  public void testPhrasePrefixWithBooleanQuery() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    add("This is a test", "object", writer);
    add("a note", "note", writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    
    // This query will be equivalent to +type:note +body:"a t*"
    BooleanQuery q = new BooleanQuery();
    q.add(new TermQuery(new Term("type", "note")), BooleanClause.Occur.MUST);
    
    MultiPhraseQuery trouble = new MultiPhraseQuery();
    trouble.add(new Term("body", "a"));
    trouble
        .add(new Term[] {new Term("body", "test"), new Term("body", "this")});
    q.add(trouble, BooleanClause.Occur.MUST);
    
    // exception will be thrown here without fix for #35626:
    ScoreDoc[] hits = searcher.search(q, null, 1000).scoreDocs;
    assertEquals("Wrong number of hits", 0, hits.length);
    writer.close();
    searcher.close();
    reader.close();
    indexStore.close();
  }
  
  public void testNoDocs() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    add("a note", "note", writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(new Term("body", "a"));
    q.add(new Term[] {new Term("body", "nope"), new Term("body", "nope")});
    assertEquals("Wrong number of hits", 0,
        searcher.search(q, null, 1).totalHits);
    
    // just make sure no exc:
    searcher.explain(q, 0);
    
    writer.close();
    searcher.close();
    reader.close();
    indexStore.close();
  }
  
  public void testHashCodeAndEquals() {
    MultiPhraseQuery query1 = new MultiPhraseQuery();
    MultiPhraseQuery query2 = new MultiPhraseQuery();
    
    assertEquals(query1.hashCode(), query2.hashCode());
    assertEquals(query1, query2);
    
    Term term1 = new Term("someField", "someText");
    
    query1.add(term1);
    query2.add(term1);
    
    assertEquals(query1.hashCode(), query2.hashCode());
    assertEquals(query1, query2);
    
    Term term2 = new Term("someField", "someMoreText");
    
    query1.add(term2);
    
    assertFalse(query1.hashCode() == query2.hashCode());
    assertFalse(query1.equals(query2));
    
    query2.add(term2);
    
    assertEquals(query1.hashCode(), query2.hashCode());
    assertEquals(query1, query2);
  }
  
  private void add(String s, String type, RandomIndexWriter writer)
      throws IOException {
    Document doc = new Document();
    doc.add(newField("body", s, Field.Store.YES, Field.Index.ANALYZED));
    doc.add(newField("type", type, Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
  }
  
  // LUCENE-2526
  public void testEmptyToString() {
    new MultiPhraseQuery().toString();
  }
  
  public void testCustomIDF() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    add("This is a test", "object", writer);
    add("a note", "note", writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new DefaultSimilarity() {
      
      @Override
      public IDFExplanation idfExplain(Collection<Term> terms,
          Searcher searcher) throws IOException {
        return new IDFExplanation() {

          @Override
          public float getIdf() {
            return 10f;
          }

          @Override
          public String explain() {
            return "just a test";
          }
          
        };
      }   
    });
    
    MultiPhraseQuery query = new MultiPhraseQuery();
    query.add(new Term[] { new Term("body", "this"), new Term("body", "that") });
    query.add(new Term("body", "is"));
    Weight weight = query.createWeight(searcher);
    assertEquals(10f * 10f, weight.sumOfSquaredWeights(), 0.001f);

    writer.close();
    searcher.close();
    reader.close();
    indexStore.close();
  }
}
