package org.apache.lucene.server;

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

import java.io.File;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONObject;

public class TestSearch extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = true;
    curIndexName = "index";
    startServer();
    createAndStartIndex();
    registerFields();
    commit();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: WhitespaceAnalyzer, matchVersion: LUCENE_43}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "title", "{type: text, highlight: true, store: true, analyzer: {class: WhitespaceAnalyzer, matchVersion: LUCENE_43}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "id", "{type: int, store: true, sort: true}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);
  }

  public void testPhraseQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'the wizard of oz'}}"), "indexGen");
    JSONObject result = send("search", "{query: {class: PhraseQuery, field: body, terms: [wizard, of, oz]}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));

    result = send("search", "{query: {class: PhraseQuery, field: body, terms: [wizard, oz]}, searcher: {indexGen: " + gen + "}}");
    assertEquals(0, getInt(result, "totalHits"));

    result = send("search", "{query: {class: PhraseQuery, field: body, terms: [wizard, oz], slop: 1}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
  }

  public void testConstantScoreQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'the wizard of oz'}}"), "indexGen");
    JSONObject result = send("search", "{query: {class: TermQuery, field: body, term: wizard}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));

    result = send("search", "{query: {class: ConstantScoreQuery, boost: 10.0, query: {class: TermQuery, field: body, term: wizard}}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
    assertEquals(10.0, getFloat(result, "hits[0].score"), .000001f);
  }

  public void testRegexpQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'testing'}}"), "indexGen");
    JSONObject r = send("search", "{query: {class: RegexpQuery, field: body, regexp: '.*est.*'}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(r, "totalHits"));
    r = send("search", "{query: {class: RegexpQuery, field: body, regexp: '.*zest.*'}, searcher: {indexGen: " + gen + "}}");
    assertEquals(0, getInt(r, "totalHits"));
  }

  public void testTermRangeQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {body: 'terma'}}");
    send("addDocument", "{fields: {body: 'termb'}}");
    long gen = getLong(send("addDocument", "{fields: {body: 'termc'}}"), "indexGen");

    JSONObject result = send("search", "{query: {class: TermRangeQuery, field: body, lowerTerm: terma, upperTerm: termc, includeLower: true, includeUpper: true}, searcher: {indexGen: " + gen + "}}");
    assertEquals(3, getInt(result, "totalHits"));
    result = send("search", "{query: {class: TermRangeQuery, field: body, lowerTerm: terma, upperTerm: termc, includeLower: false, includeUpper: false}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
  }

  public void testMatchAllDocsQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {body: 'terma'}}");
    send("addDocument", "{fields: {body: 'termb'}}");
    long gen = getLong(send("addDocument", "{fields: {body: 'termc'}}"), "indexGen");
    assertEquals(3, getInt(send("search", "{query: {class: MatchAllDocsQuery}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testWildcardQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {body: 'terma'}}");
    send("addDocument", "{fields: {body: 'termb'}}");
    long gen = getLong(send("addDocument", "{fields: {body: 'termc'}}"), "indexGen");
    assertEquals(3, getInt(send("search", "{query: {class: WildcardQuery, field: body, term: 'term?'}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testFuzzyQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'fantastic'}}"), "indexGen");
    assertEquals(1, getInt(send("search", "{query: {class: FuzzyQuery, field: body, term: 'fantasic', maxEdits: 1}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{query: {class: FuzzyQuery, field: body, term: 'fantasic', maxEdits: 2}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{query: {class: FuzzyQuery, field: body, term: 'fantasc', maxEdits: 1}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{query: {class: FuzzyQuery, field: body, term: 'fantasc', maxEdits: 2}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{query: {class: FuzzyQuery, field: body, term: 'fantasc', maxEdits: 2, prefixLength: 4}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testCommonTermsQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {body: 'fantastic'}}");
    send("addDocument", "{fields: {body: 'fantastic four'}}");
    long gen = getLong(send("addDocument", "{fields: {body: 'fantastic five'}}"), "indexGen");

    assertEquals(1, getInt(send("search", "{query: {class: CommonTermsQuery, highFreqOccur: must, lowFreqOccur: must, maxTermFrequency: 0.5, field: body, terms: [fantastic, four]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testMultiPhraseQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {body: 'fantastic five is furious'}}");
    long gen = getLong(send("addDocument", "{fields: {body: 'fantastic four is furious'}}"), "indexGen");

    assertEquals(1, getInt(send("search", "{query: {class: MultiPhraseQuery, field: body, terms: [fantastic, five, is, furious]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(2, getInt(send("search", "{query: {class: MultiPhraseQuery, field: body, terms: [fantastic, {term: furious, position: 3}]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(2, getInt(send("search", "{query: {class: MultiPhraseQuery, field: body, terms: [fantastic, [five, four], {term: furious, position: 3}]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testClassicQPDefaultOperator() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'fantastic four is furious'}}"), "indexGen");
    
    assertEquals(1, getInt(send("search", "{queryParser: {class: classic, defaultOperator: or, defaultField: body}, queryText: 'furious five', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{queryParser: {class: classic, defaultOperator: and, defaultField: body}, queryText: 'furious five', searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testMultiFieldQueryParser() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'fantastic four is furious', title: 'here is the title'}}"), "indexGen");
    
    assertEquals(1, getInt(send("search", "{queryParser: {class: MultiFieldQueryParser, defaultOperator: or, fields: [body, {field: title, boost: 2.0}]}, queryText: 'title furious', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{queryParser: {class: MultiFieldQueryParser, defaultOperator: and, fields: [body, {field: title, boost: 2.0}]}, queryText: 'title furious', searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testSimpleQueryParser() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {body: 'fantastic four is furious', title: 'here is the title'}}"), "indexGen");
    
    assertEquals(1, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: or, fields: [body, title], operators: [WHITESPACE]}, queryText: 'title furious', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: and, fields: [body, title], operators: [WHITESPACE]}, queryText: 'title furious', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: or, fields: [body, title], operators: [WHITESPACE]}, queryText: 't* f*', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: or, fields: [body, title], operators: [WHITESPACE, PREFIX]}, queryText: 't* f*', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: or, fields: [body, title], operators: [WHITESPACE]}, queryText: '\"furious title\"', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: or, fields: [body, title], operators: [WHITESPACE, PHRASE]}, queryText: '\"furious title\"', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{queryParser: {class: SimpleQueryParser, defaultOperator: or, fields: [body, title], operators: [WHITESPACE, PHRASE]}, queryText: '\"fantastic four\"', searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testNumericRangeQuery() throws Exception {
    curIndexName = "nrq";
    for(String type : new String[] {"int", "long", "float", "double"}) {
      TestUtil.rmDir(new File("nrq"));
      send("createIndex", "{rootDir: nrq}");
      send("startIndex");
      send("registerFields", String.format(Locale.ROOT, "{fields: {nf: {type: %s, search: true}}}", type));
      send("addDocument", "{fields: {nf: 5}}");
      send("addDocument", "{fields: {nf: 10}}");
      long gen = getLong(send("addDocument", "{fields: {nf: 17}}"), "indexGen");

      // Both min & max:
      assertEquals(3, getInt(send("search",

                                  String.format(Locale.ROOT, "{query: {class: NumericRangeQuery, field: nf, min: 5, max: 17, minInclusive: true, maxInclusive: true}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave min out:
      assertEquals(3, getInt(send("search",
                                  String.format(Locale.ROOT, "{query: {class: NumericRangeQuery, field: nf, max: 17, maxInclusive: true}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave min out, don't include max:
      assertEquals(2, getInt(send("search",
                                  String.format(Locale.ROOT, "{query: {class: NumericRangeQuery, field: nf, max: 17, maxInclusive: false}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave max out:
      assertEquals(3, getInt(send("search",
                                  String.format(Locale.ROOT, "{query: {class: NumericRangeQuery, field: nf, min: 5, minInclusive: true}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave max out, don't include max:
      assertEquals(2, getInt(send("search",
                                  String.format(Locale.ROOT, "{query: {class: NumericRangeQuery, field: nf, min: 5, minInclusive: false}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));
      send("stopIndex");
      send("deleteIndex");
    }
  }

  public void testSearchAfter() throws Exception {
    deleteAllDocs();
    long gen = 0;
    for(int i=0;i<20;i++) {
      gen = getLong(send("addDocument", "{fields: {body: 'this is the body', id: " + i + "}}"), "indexGen");
    }

    JSONObject lastPage = null;

    Set<Integer> seenIDs = new HashSet<Integer>();

    // Pull 4 pages with 5 hits per page:
    for(int i=0;i<4;i++) {
      String sa;
      if (lastPage != null) {
        sa = ", searchAfter: {lastDoc: " + getInt(lastPage, "searchState.lastDoc") + ", lastScore: " + getFloat(lastPage, "searchState.lastScore") + "}";
      } else {
        sa = "";
      }

      lastPage = send("search", "{query: MatchAllDocsQuery, topHits: 5, retrieveFields: [id], searcher: {indexGen: " + gen + "}" + sa + "}");
      //System.out.println("i=" + i + ": " + lastPage);

      // 20 total hits
      assertEquals(20, getInt(lastPage, "totalHits"));
      assertEquals(5, getInt(lastPage, "hits.length"));
      for(int j=0;j<5;j++) {
        seenIDs.add(getInt(lastPage, "hits[" + j + "].fields.id"));
      }
    }

    assertEquals(20, seenIDs.size());
  }

  public void testSearchWithTimeout() throws Exception {
    deleteAllDocs();
    LineFileDocs docs = new LineFileDocs(random());
    long charCountLimit = atLeast(10*1024);
    long charCount = 0;
    int id = 0;
    while (charCount < charCountLimit) {
      Document doc = docs.nextDoc();
      charCount += doc.get("body").length() + doc.get("titleTokenized").length();
      JSONObject fields = new JSONObject();
      fields.put("title", doc.get("titleTokenized"));
      fields.put("body", doc.get("body"));
      fields.put("id", id++);
      JSONObject o = new JSONObject();
      o.put("fields", fields);
      send("addDocument", o);
    }

    assertFailsWith("search",
                    "{queryText: the, retrieveFields: [id], timeoutSec: 0.0}",
                    "search > timeoutSec: must be > 0 msec");

    // NOTE: does not actually test that we hit the timeout;
    // just that we can specify timeoutSec
    send("search", "{queryText: the, retrieveFields: [id], timeoutSec: 0.001}");
  }

  public void testSearchAfterWithSort() throws Exception {
    deleteAllDocs();
    long gen = 0;
    for(int i=0;i<20;i++) {
      gen = getLong(send("addDocument", "{fields: {body: 'this is the body', id: " + i + "}}"), "indexGen");
    }

    JSONObject lastPage = null;

    Set<Integer> seenIDs = new HashSet<Integer>();

    // Pull 4 pages with 5 hits per page:
    for(int i=0;i<4;i++) {
      String sa;
      JSONObject o = new JSONObject();
      o.put("query", "MatchAllDocsQuery");
      o.put("topHits", 5);
      put(o, "retrieveFields", "[id]");
      put(o, "sort", "{fields: [{field: id}]}");
      put(o, "searcher", "{indexGen: " + gen + "}");
      if (lastPage != null) {
        JSONObject o2 = new JSONObject();
        o.put("searchAfter", o2);
        o2.put("lastDoc", getInt(lastPage, "searchState.lastDoc"));
        o2.put("lastFieldValues", get(lastPage, "searchState.lastFieldValues"));
      } else {
        sa = "";
      }

      lastPage = send("search", o);

      // 20 total hits
      assertEquals(20, getInt(lastPage, "totalHits"));
      assertEquals(5, getInt(lastPage, "hits.length"));
      for(int j=0;j<5;j++) {
        seenIDs.add(getInt(lastPage, "hits[" + j + "].fields.id"));
      }
    }

    assertEquals(20, seenIDs.size());
  }

  public void testRecencyBlendedSort() throws Exception {
    curIndexName = "recency";
    File dir = new File(TestUtil.getTempDir("recency"), "root");
    send("createIndex", "{rootDir: " + dir.getAbsolutePath() + "}");
    send("startIndex");
    send("registerFields", "{fields: {timestamp: {type: long, search: false, sort: true}, body: {type: text, analyzer: StandardAnalyzer}}}");

    long t = System.currentTimeMillis()/1000;
    send("addDocument", "{fields: {body: 'this is some text', timestamp: " + (t-100) + "}}");
    long gen = getLong(send("addDocument", "{fields: {body: 'this is some text', timestamp: " + t + "}}"), "indexGen");

    for(int pass=0;pass<2;pass++) {
      // Unboosted:
      JSONObject result = send("search", "{queryText: text, searcher: {indexGen: " + gen + "}}");
      assertEquals(2, getInt(result, "totalHits"));
      assertEquals(0, getInt(result, "hits[0].doc"));
      assertEquals(1, getInt(result, "hits[1].doc"));

      // Blended relevance + recency changes the order:
      t = System.currentTimeMillis()/1000;
      // nocommit this isn't right: the boost gets down to 0
      // when it's "recent"
      result = send("search",
                    "{queryText: text, virtualFields: [" + 
                    "{name: age,   expression: '" + t + " - timestamp'}, " + 
                    "{name: boost, expression: '(age >= 30) ? 1.0 : (2.0 * (30. - age) / 30)'}, " +
                    "{name: blend, expression: 'boost * _score'}], " + 
                    " sort: {fields: [{field: blend, reverse: true}]}, retrieveFields: [age, boost], searcher: {indexGen: " + gen + "}}");
      assertEquals(2, getInt(result, "totalHits"));
      assertEquals(1, getInt(result, "hits[0].doc"));
      assertEquals(0, getInt(result, "hits[1].doc"));
      assertTrue(getFloat(result, "hits[0].fields.boost") > 1.0f);
      assertEquals(1.0, getFloat(result, "hits[1].fields.boost"), 0.0001f);

      // Make sure this survives restart:
      send("stopIndex");
      send("startIndex");
    }
  }

  public void testFailsQueryParserError() throws Exception {
    assertFailsWith("search",
                    "{queryText: 'field:bar', retrieveFields: [id],}",
                    "field \"field\" is unknown");
  }
}
