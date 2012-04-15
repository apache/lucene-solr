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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;

/**
 * This class tests the MultiPhraseQuery class.
 * 
 * 
 */
public class TestMultiPhraseQuery extends LuceneTestCase {
  
  public void testPhrasePrefix() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
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
    
    // this TermEnum gives "piccadilly", "pie" and "pizza".
    String prefix = "pi";
    TermsEnum te = MultiFields.getFields(reader).terms("body").iterator(null);
    te.seekCeil(new BytesRef(prefix));
    do {
      String s = te.term().utf8ToString();
      if (s.startsWith(prefix)) {
        termsWithPrefix.add(new Term("body", s));
      } else {
        break;
      }
    } while (te.next() != null);
    
    query1.add(termsWithPrefix.toArray(new Term[0]));
    assertEquals("body:\"blueberry (piccadilly pie pizza)\"", query1.toString());
    query2.add(termsWithPrefix.toArray(new Term[0]));
    assertEquals("body:\"strawberry (piccadilly pie pizza)\"", query2
        .toString());
    
    ScoreDoc[] result;
    result = searcher.search(query1, null, 1000).scoreDocs;
    assertEquals(2, result.length);
    result = searcher.search(query2, null, 1000).scoreDocs;
    assertEquals(0, result.length);
    
    // search for "blue* pizza":
    MultiPhraseQuery query3 = new MultiPhraseQuery();
    termsWithPrefix.clear();
    prefix = "blue";
    te.seekCeil(new BytesRef(prefix));
    
    do {
      if (te.term().utf8ToString().startsWith(prefix)) {
        termsWithPrefix.add(new Term("body", te.term().utf8ToString()));
      }
    } while (te.next() != null);
    
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
    
    assertEquals(3, result.length); // blueberry pizza, bluebird pizza, bluebird
                                    // foobar pizza
    
    MultiPhraseQuery query4 = new MultiPhraseQuery();
    try {
      query4.add(new Term("field1", "foo"));
      query4.add(new Term("field2", "foobar"));
      fail();
    } catch (IllegalArgumentException e) {
      // okay, all terms must belong to the same field
    }
    
    writer.close();
    reader.close();
    indexStore.close();
  }

  // LUCENE-2580
  public void testTall() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
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
    r.close();
    indexStore.close();
  }
  
  @Ignore //LUCENE-3821 fixes sloppy phrase scoring, except for this known problem 
  public void testMultiSloppyWithRepeats() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    add("a b c d e f g h i k", writer);
    IndexReader r = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(r);
    
    MultiPhraseQuery q = new MultiPhraseQuery();
    // this will fail, when the scorer would propagate [a] rather than [a,b],
    q.add(new Term[] {new Term("body", "a"), new Term("body", "b")});
    q.add(new Term[] {new Term("body", "a")});
    q.setSlop(6);
    assertEquals(1, searcher.search(q, 1).totalHits); // should match on "a b"
    
    r.close();
    indexStore.close();
  }

  public void testMultiExactWithRepeats() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    add("a b c d e f g h i k", writer);
    IndexReader r = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(r);
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(new Term[] {new Term("body", "a"), new Term("body", "d")}, 0);
    q.add(new Term[] {new Term("body", "a"), new Term("body", "f")}, 2);
    assertEquals(1, searcher.search(q, 1).totalHits); // should match on "a b"
    r.close();
    indexStore.close();
  }
  
  private void add(String s, RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newField("body", s, TextField.TYPE_STORED));
    writer.addDocument(doc);
  }
  
  public void testBooleanQueryContainingSingleTermPrefixQuery()
      throws IOException {
    // this tests against bug 33161 (now fixed)
    // In order to cause the bug, the outer query must have more than one term
    // and all terms required.
    // The contained PhraseMultiQuery must contain exactly one term array.
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
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
    reader.close();
    indexStore.close();
  }
  
  public void testPhrasePrefixWithBooleanQuery() throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
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
    reader.close();
    indexStore.close();
  }
  
  public void testNoDocs() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
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
    doc.add(newField("body", s, TextField.TYPE_STORED));
    doc.add(newField("type", type, StringField.TYPE_UNSTORED));
    writer.addDocument(doc);
  }
  
  // LUCENE-2526
  public void testEmptyToString() {
    new MultiPhraseQuery().toString();
  }
  
  public void testCustomIDF() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    add("This is a test", "object", writer);
    add("a note", "note", writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new DefaultSimilarity() { 
      @Override
      public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
        return new Explanation(10f, "just a test");
      } 
    });
    
    MultiPhraseQuery query = new MultiPhraseQuery();
    query.add(new Term[] { new Term("body", "this"), new Term("body", "that") });
    query.add(new Term("body", "is"));
    Weight weight = query.createWeight(searcher);
    assertEquals(10f * 10f, weight.getValueForNormalization(), 0.001f);

    writer.close();
    reader.close();
    indexStore.close();
  }

  public void testZeroPosIncr() throws IOException {
    Directory dir = new RAMDirectory();
    final Token[] tokens = new Token[3];
    tokens[0] = new Token();
    tokens[0].append("a");
    tokens[0].setPositionIncrement(1);
    tokens[1] = new Token();
    tokens[1].append("b");
    tokens[1].setPositionIncrement(0);
    tokens[2] = new Token();
    tokens[2].append("c");
    tokens[2].setPositionIncrement(0);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new TextField("field", new CannedTokenStream(tokens)));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", new CannedTokenStream(tokens)));
    writer.addDocument(doc);
    IndexReader r = writer.getReader();
    writer.close();
    IndexSearcher s = new IndexSearcher(r);
    MultiPhraseQuery mpq = new MultiPhraseQuery();
    //mpq.setSlop(1);

    // NOTE: not great that if we do the else clause here we
    // get different scores!  MultiPhraseQuery counts that
    // phrase as occurring twice per doc (it should be 1, I
    // think?).  This is because MultipleTermPositions is able to
    // return the same position more than once (0, in this
    // case):
    if (true) {
      mpq.add(new Term[] {new Term("field", "b"), new Term("field", "c")}, 0);
      mpq.add(new Term[] {new Term("field", "a")}, 0);
    } else {
      mpq.add(new Term[] {new Term("field", "a")}, 0);
      mpq.add(new Term[] {new Term("field", "b"), new Term("field", "c")}, 0);
    }
    TopDocs hits = s.search(mpq, 2);
    assertEquals(2, hits.totalHits);
    assertEquals(hits.scoreDocs[0].score, hits.scoreDocs[1].score, 1e-5);
    /*
    for(int hit=0;hit<hits.totalHits;hit++) {
      ScoreDoc sd = hits.scoreDocs[hit];
      System.out.println("  hit doc=" + sd.doc + " score=" + sd.score);
    }
    */
    r.close();
    dir.close();
  }

  private static Token makeToken(String text, int posIncr) {
    final Token t = new Token();
    t.append(text);
    t.setPositionIncrement(posIncr);
    return t;
  }

  private final static Token[] INCR_0_DOC_TOKENS = new Token[] {
    makeToken("x", 1),
    makeToken("a", 1),
    makeToken("1", 0),
    makeToken("m", 1),  // not existing, relying on slop=2
    makeToken("b", 1),
    makeToken("1", 0),
    makeToken("n", 1), // not existing, relying on slop=2
    makeToken("c", 1),
    makeToken("y", 1)
  };
  
  private final static Token[] INCR_0_QUERY_TOKENS_AND = new Token[] {
    makeToken("a", 1),
    makeToken("1", 0),
    makeToken("b", 1),
    makeToken("1", 0),
    makeToken("c", 1)
  };
  
  private final static Token[][] INCR_0_QUERY_TOKENS_AND_OR_MATCH = new Token[][] {
    { makeToken("a", 1) },
    { makeToken("x", 1), makeToken("1", 0) },
    { makeToken("b", 2) },
    { makeToken("x", 2), makeToken("1", 0) },
    { makeToken("c", 3) }
  };
  
  private final static Token[][] INCR_0_QUERY_TOKENS_AND_OR_NO_MATCHN = new Token[][] {
    { makeToken("x", 1) },
    { makeToken("a", 1), makeToken("1", 0) },
    { makeToken("x", 2) },
    { makeToken("b", 2), makeToken("1", 0) },
    { makeToken("c", 3) }
  };
  
  /**
   * using query parser, MPQ will be created, and will not be strict about having all query terms 
   * in each position - one of each position is sufficient (OR logic)
   */
  public void testZeroPosIncrSloppyParsedAnd() throws IOException {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(new Term[]{ new Term("field", "a"), new Term("field", "1") }, -1);
    q.add(new Term[]{ new Term("field", "b"), new Term("field", "1") }, 0);
    q.add(new Term[]{ new Term("field", "c") }, 1);
    doTestZeroPosIncrSloppy(q, 0);
    q.setSlop(1);
    doTestZeroPosIncrSloppy(q, 0);
    q.setSlop(2);
    doTestZeroPosIncrSloppy(q, 1);
  }
  
  private void doTestZeroPosIncrSloppy(Query q, int nExpected) throws IOException {
    Directory dir = newDirectory(); // random dir
    IndexWriterConfig cfg = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    IndexWriter writer = new IndexWriter(dir, cfg);
    Document doc = new Document();
    doc.add(new TextField("field", new CannedTokenStream(INCR_0_DOC_TOKENS)));
    writer.addDocument(doc);
    IndexReader r = IndexReader.open(writer,false);
    writer.close();
    IndexSearcher s = new IndexSearcher(r);
    
    if (VERBOSE) {
      System.out.println("QUERY=" + q);
    }
    
    TopDocs hits = s.search(q, 1);
    assertEquals("wrong number of results", nExpected, hits.totalHits);
    
    if (VERBOSE) {
      for(int hit=0;hit<hits.totalHits;hit++) {
        ScoreDoc sd = hits.scoreDocs[hit];
        System.out.println("  hit doc=" + sd.doc + " score=" + sd.score);
      }
    }
    
    r.close();
    dir.close();
  }

  /**
   * PQ AND Mode - Manually creating a phrase query
   */
  public void testZeroPosIncrSloppyPqAnd() throws IOException {
    final PhraseQuery pq = new PhraseQuery();
    int pos = -1;
    for (Token tap : INCR_0_QUERY_TOKENS_AND) {
      pos += tap.getPositionIncrement();
      pq.add(new Term("field",tap.toString()), pos);
    }
    doTestZeroPosIncrSloppy(pq, 0);
    pq.setSlop(1);
    doTestZeroPosIncrSloppy(pq, 0);
    pq.setSlop(2);
    doTestZeroPosIncrSloppy(pq, 1);
  }

  /**
   * MPQ AND Mode - Manually creating a multiple phrase query
   */
  public void testZeroPosIncrSloppyMpqAnd() throws IOException {
    final MultiPhraseQuery mpq = new MultiPhraseQuery();
    int pos = -1;
    for (Token tap : INCR_0_QUERY_TOKENS_AND) {
      pos += tap.getPositionIncrement();
      mpq.add(new Term[]{new Term("field",tap.toString())}, pos); //AND logic
    }
    doTestZeroPosIncrSloppy(mpq, 0);
    mpq.setSlop(1);
    doTestZeroPosIncrSloppy(mpq, 0);
    mpq.setSlop(2);
    doTestZeroPosIncrSloppy(mpq, 1);
  }

  /**
   * MPQ Combined AND OR Mode - Manually creating a multiple phrase query
   */
  public void testZeroPosIncrSloppyMpqAndOrMatch() throws IOException {
    final MultiPhraseQuery mpq = new MultiPhraseQuery();
    for (Token tap[] : INCR_0_QUERY_TOKENS_AND_OR_MATCH) {
      Term[] terms = tapTerms(tap);
      final int pos = tap[0].getPositionIncrement()-1;
      mpq.add(terms, pos); //AND logic in pos, OR across lines 
    }
    doTestZeroPosIncrSloppy(mpq, 0);
    mpq.setSlop(1);
    doTestZeroPosIncrSloppy(mpq, 0);
    mpq.setSlop(2);
    doTestZeroPosIncrSloppy(mpq, 1);
  }

  /**
   * MPQ Combined AND OR Mode - Manually creating a multiple phrase query - with no match
   */
  public void testZeroPosIncrSloppyMpqAndOrNoMatch() throws IOException {
    final MultiPhraseQuery mpq = new MultiPhraseQuery();
    for (Token tap[] : INCR_0_QUERY_TOKENS_AND_OR_NO_MATCHN) {
      Term[] terms = tapTerms(tap);
      final int pos = tap[0].getPositionIncrement()-1;
      mpq.add(terms, pos); //AND logic in pos, OR across lines 
    }
    doTestZeroPosIncrSloppy(mpq, 0);
    mpq.setSlop(2);
    doTestZeroPosIncrSloppy(mpq, 0);
  }

  private Term[] tapTerms(Token[] tap) {
    Term[] terms = new Term[tap.length];
    for (int i=0; i<terms.length; i++) {
      terms[i] = new Term("field",tap[i].toString());
    }
    return terms;
  }
  
}
