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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
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
    MultiPhraseQuery.Builder query1builder = new MultiPhraseQuery.Builder();
    // search for "strawberry pi*":
    MultiPhraseQuery.Builder query2builder = new MultiPhraseQuery.Builder();
    query1builder.add(new Term("body", "blueberry"));
    query2builder.add(new Term("body", "strawberry"));
    
    LinkedList<Term> termsWithPrefix = new LinkedList<>();
    
    // this TermEnum gives "piccadilly", "pie" and "pizza".
    String prefix = "pi";
    TermsEnum te = MultiTerms.getTerms(reader,"body").iterator();
    te.seekCeil(new BytesRef(prefix));
    do {
      String s = te.term().utf8ToString();
      if (s.startsWith(prefix)) {
        termsWithPrefix.add(new Term("body", s));
      } else {
        break;
      }
    } while (te.next() != null);
    
    query1builder.add(termsWithPrefix.toArray(new Term[0]));
    MultiPhraseQuery query1 = query1builder.build();
    assertEquals("body:\"blueberry (piccadilly pie pizza)\"", query1.toString());
    
    query2builder.add(termsWithPrefix.toArray(new Term[0]));
    MultiPhraseQuery query2 = query2builder.build();
    assertEquals("body:\"strawberry (piccadilly pie pizza)\"", query2.toString());
    
    ScoreDoc[] result;
    result = searcher.search(query1, 1000).scoreDocs;
    assertEquals(2, result.length);
    result = searcher.search(query2, 1000).scoreDocs;
    assertEquals(0, result.length);
    
    // search for "blue* pizza":
    MultiPhraseQuery.Builder query3builder = new MultiPhraseQuery.Builder();
    termsWithPrefix.clear();
    prefix = "blue";
    te.seekCeil(new BytesRef(prefix));
    
    do {
      if (te.term().utf8ToString().startsWith(prefix)) {
        termsWithPrefix.add(new Term("body", te.term().utf8ToString()));
      }
    } while (te.next() != null);
    
    query3builder.add(termsWithPrefix.toArray(new Term[0]));
    query3builder.add(new Term("body", "pizza"));
    
    MultiPhraseQuery query3 = query3builder.build();
    
    result = searcher.search(query3, 1000).scoreDocs;
    assertEquals(2, result.length); // blueberry pizza, bluebird pizza
    assertEquals("body:\"(blueberry bluebird) pizza\"", query3.toString());
    
    // test slop:
    query3builder.setSlop(1);
    query3 = query3builder.build();
    result = searcher.search(query3, 1000).scoreDocs;
    
    // just make sure no exc:
    searcher.explain(query3, 0);
    
    assertEquals(3, result.length); // blueberry pizza, bluebird pizza, bluebird
                                    // foobar pizza
    
    MultiPhraseQuery.Builder query4builder = new MultiPhraseQuery.Builder();
    expectThrows(IllegalArgumentException.class, () -> {
      query4builder.add(new Term("field1", "foo"));
      query4builder.add(new Term("field2", "foobar"));
    });
    
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
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(new Term("body", "blueberry"));
    qb.add(new Term("body", "chocolate"));
    qb.add(new Term[] {new Term("body", "pie"), new Term("body", "tart")});
    assertEquals(2, searcher.count(qb.build()));
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
    
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    // this will fail, when the scorer would propagate [a] rather than [a,b],
    qb.add(new Term[] {new Term("body", "a"), new Term("body", "b")});
    qb.add(new Term[] {new Term("body", "a")});
    qb.setSlop(6);
    assertEquals(1, searcher.count(qb.build())); // should match on "a b"
    
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
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(new Term[] {new Term("body", "a"), new Term("body", "d")}, 0);
    qb.add(new Term[] {new Term("body", "a"), new Term("body", "f")}, 2);
    assertEquals(1, searcher.count(qb.build())); // should match on "a b"
    r.close();
    indexStore.close();
  }
  
  private void add(String s, RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("body", s, Field.Store.YES));
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
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new TermQuery(new Term("body", "pie")), BooleanClause.Occur.MUST);
    
    MultiPhraseQuery.Builder troubleBuilder = new MultiPhraseQuery.Builder();
    troubleBuilder.add(new Term[] {new Term("body", "blueberry"),
        new Term("body", "blue")});
    q.add(troubleBuilder.build(), BooleanClause.Occur.MUST);
    
    // exception will be thrown here without fix
    ScoreDoc[] hits = searcher.search(q.build(), 1000).scoreDocs;
    
    assertEquals("Wrong number of hits", 2, hits.length);
    
    // just make sure no exc:
    searcher.explain(q.build(), 0);
    
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
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new TermQuery(new Term("type", "note")), BooleanClause.Occur.MUST);
    
    MultiPhraseQuery.Builder troubleBuilder = new MultiPhraseQuery.Builder();
    troubleBuilder.add(new Term("body", "a"));
    troubleBuilder
        .add(new Term[] {new Term("body", "test"), new Term("body", "this")});
    q.add(troubleBuilder.build(), BooleanClause.Occur.MUST);
    
    // exception will be thrown here without fix for #35626:
    ScoreDoc[] hits = searcher.search(q.build(), 1000).scoreDocs;
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
    
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(new Term("body", "a"));
    qb.add(new Term[] {new Term("body", "nope"), new Term("body", "nope")});
    MultiPhraseQuery q = qb.build();
    assertEquals("Wrong number of hits", 0,
        searcher.count(q));
    
    // just make sure no exc:
    searcher.explain(q, 0);
    
    writer.close();
    reader.close();
    indexStore.close();
  }
  
  public void testHashCodeAndEquals() {
    MultiPhraseQuery.Builder query1builder = new MultiPhraseQuery.Builder();
    MultiPhraseQuery query1 = query1builder.build();
    
    MultiPhraseQuery.Builder query2builder = new MultiPhraseQuery.Builder();
    MultiPhraseQuery query2 = query2builder.build();
    
    assertEquals(query1.hashCode(), query2.hashCode());
    assertEquals(query1, query2);
    
    Term term1 = new Term("someField", "someText");
    
    query1builder.add(term1);
    query1 = query1builder.build();

    query2builder.add(term1);
    query2 = query2builder.build();
    
    assertEquals(query1.hashCode(), query2.hashCode());
    assertEquals(query1, query2);
    
    Term term2 = new Term("someField", "someMoreText");
    
    query1builder.add(term2);
    query1 = query1builder.build();
    
    assertFalse(query1.hashCode() == query2.hashCode());
    assertFalse(query1.equals(query2));
    
    query2builder.add(term2);
    query2 = query2builder.build();
    
    assertEquals(query1.hashCode(), query2.hashCode());
    assertEquals(query1, query2);
  }
  
  private void add(String s, String type, RandomIndexWriter writer)
      throws IOException {
    Document doc = new Document();
    doc.add(newTextField("body", s, Field.Store.YES));
    doc.add(newStringField("type", type, Field.Store.NO));
    writer.addDocument(doc);
  }
  
  // LUCENE-2526
  public void testEmptyToString() {
    new MultiPhraseQuery.Builder().build().toString();
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
    IndexSearcher s = newSearcher(r);
    MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
    //mpq.setSlop(1);

    // NOTE: not great that if we do the else clause here we
    // get different scores!  MultiPhraseQuery counts that
    // phrase as occurring twice per doc (it should be 1, I
    // think?).  This is because MultipleTermPositions is able to
    // return the same position more than once (0, in this
    // case):
    if (true) {
      mpqb.add(new Term[] {new Term("field", "b"), new Term("field", "c")}, 0);
      mpqb.add(new Term[] {new Term("field", "a")}, 0);
    } else {
      mpqb.add(new Term[] {new Term("field", "a")}, 0);
      mpqb.add(new Term[] {new Term("field", "b"), new Term("field", "c")}, 0);
    }
    TopDocs hits = s.search(mpqb.build(), 2);
    assertEquals(2, hits.totalHits.value);
    assertEquals(hits.scoreDocs[0].score, hits.scoreDocs[1].score, 1e-5);
    /*
    for(int hit=0;hit<hits.totalHits.value;hit++) {
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
    MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();
    qb.add(new Term[]{ new Term("field", "a"), new Term("field", "1") }, -1);
    qb.add(new Term[]{ new Term("field", "b"), new Term("field", "1") }, 0);
    qb.add(new Term[]{ new Term("field", "c") }, 1);
    doTestZeroPosIncrSloppy(qb.build(), 0);
    qb.setSlop(1);
    doTestZeroPosIncrSloppy(qb.build(), 0);
    qb.setSlop(2);
    doTestZeroPosIncrSloppy(qb.build(), 1);
  }
  
  private void doTestZeroPosIncrSloppy(Query q, int nExpected) throws IOException {
    Directory dir = newDirectory(); // random dir
    IndexWriterConfig cfg = newIndexWriterConfig(null);
    IndexWriter writer = new IndexWriter(dir, cfg);
    Document doc = new Document();
    doc.add(new TextField("field", new CannedTokenStream(INCR_0_DOC_TOKENS)));
    writer.addDocument(doc);
    IndexReader r = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher s = newSearcher(r);
    
    if (VERBOSE) {
      System.out.println("QUERY=" + q);
    }
    
    TopDocs hits = s.search(q, 1);
    assertEquals("wrong number of results", nExpected, hits.totalHits.value);
    
    if (VERBOSE) {
      for(int hit=0;hit<hits.totalHits.value;hit++) {
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
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    int pos = -1;
    for (Token tap : INCR_0_QUERY_TOKENS_AND) {
      pos += tap.getPositionIncrement();
      builder.add(new Term("field", tap.toString()), pos);
    }
    builder.setSlop(0);
    doTestZeroPosIncrSloppy(builder.build(), 0);
    builder.setSlop(1);
    doTestZeroPosIncrSloppy(builder.build(), 0);
    builder.setSlop(2);
    doTestZeroPosIncrSloppy(builder.build(), 1);
  }

  /**
   * MPQ AND Mode - Manually creating a multiple phrase query
   */
  public void testZeroPosIncrSloppyMpqAnd() throws IOException {
    final MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
    int pos = -1;
    for (Token tap : INCR_0_QUERY_TOKENS_AND) {
      pos += tap.getPositionIncrement();
      mpqb.add(new Term[]{new Term("field",tap.toString())}, pos); //AND logic
    }
    doTestZeroPosIncrSloppy(mpqb.build(), 0);
    mpqb.setSlop(1);
    doTestZeroPosIncrSloppy(mpqb.build(), 0);
    mpqb.setSlop(2);
    doTestZeroPosIncrSloppy(mpqb.build(), 1);
  }

  /**
   * MPQ Combined AND OR Mode - Manually creating a multiple phrase query
   */
  public void testZeroPosIncrSloppyMpqAndOrMatch() throws IOException {
    final MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
    for (Token tap[] : INCR_0_QUERY_TOKENS_AND_OR_MATCH) {
      Term[] terms = tapTerms(tap);
      final int pos = tap[0].getPositionIncrement()-1;
      mpqb.add(terms, pos); //AND logic in pos, OR across lines 
    }
    doTestZeroPosIncrSloppy(mpqb.build(), 0);
    mpqb.setSlop(1);
    doTestZeroPosIncrSloppy(mpqb.build(), 0);
    mpqb.setSlop(2);
    doTestZeroPosIncrSloppy(mpqb.build(), 1);
  }

  /**
   * MPQ Combined AND OR Mode - Manually creating a multiple phrase query - with no match
   */
  public void testZeroPosIncrSloppyMpqAndOrNoMatch() throws IOException {
    final MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
    for (Token tap[] : INCR_0_QUERY_TOKENS_AND_OR_NO_MATCHN) {
      Term[] terms = tapTerms(tap);
      final int pos = tap[0].getPositionIncrement()-1;
      mpqb.add(terms, pos); //AND logic in pos, OR across lines 
    }
    doTestZeroPosIncrSloppy(mpqb.build(), 0);
    mpqb.setSlop(2);
    doTestZeroPosIncrSloppy(mpqb.build(), 0);
  }

  private Term[] tapTerms(Token[] tap) {
    Term[] terms = new Term[tap.length];
    for (int i=0; i<terms.length; i++) {
      terms[i] = new Term("field",tap[i].toString());
    }
    return terms;
  }
  
  public void testNegativeSlop() throws Exception {
    MultiPhraseQuery.Builder queryBuilder = new MultiPhraseQuery.Builder();
    queryBuilder.add(new Term("field", "two"));
    queryBuilder.add(new Term("field", "one"));
    expectThrows(IllegalArgumentException.class, () -> {
      queryBuilder.setSlop(-2);
    });
  }
  
}
