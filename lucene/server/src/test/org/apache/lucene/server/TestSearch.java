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

import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONObject;

public class TestSearch extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    clearDir();
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
    o2.put("indexName", "index");
    send("registerFields", o2);
  }

  public void testPhraseQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'the wizard of oz'}}"), "indexGen");
    JSONObject result = send("search", "{indexName: index, query: {class: PhraseQuery, field: body, terms: [wizard, of, oz]}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));

    result = send("search", "{indexName: index, query: {class: PhraseQuery, field: body, terms: [wizard, oz]}, searcher: {indexGen: " + gen + "}}");
    assertEquals(0, getInt(result, "totalHits"));

    result = send("search", "{indexName: index, query: {class: PhraseQuery, field: body, terms: [wizard, oz], slop: 1}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
  }

  public void testConstantScoreQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'the wizard of oz'}}"), "indexGen");
    JSONObject result = send("search", "{indexName: index, query: {class: TermQuery, field: body, term: wizard}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));

    result = send("search", "{indexName: index, query: {class: ConstantScoreQuery, boost: 10.0, query: {class: TermQuery, field: body, term: wizard}}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
    assertEquals(10.0, getFloat(result, "hits[0].score"), .000001f);
  }

  public void testRegexpQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'testing'}}"), "indexGen");
    JSONObject r = send("search", "{indexName: index, query: {class: RegexpQuery, field: body, regexp: '.*est.*'}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(r, "totalHits"));
    r = send("search", "{indexName: index, query: {class: RegexpQuery, field: body, regexp: '.*zest.*'}, searcher: {indexGen: " + gen + "}}");
    assertEquals(0, getInt(r, "totalHits"));
  }

  public void testTermRangeQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{indexName: index, fields: {body: 'terma'}}");
    send("addDocument", "{indexName: index, fields: {body: 'termb'}}");
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'termc'}}"), "indexGen");

    JSONObject result = send("search", "{indexName: index, query: {class: TermRangeQuery, field: body, lowerTerm: terma, upperTerm: termc, includeLower: true, includeUpper: true}, searcher: {indexGen: " + gen + "}}");
    assertEquals(3, getInt(result, "totalHits"));
    result = send("search", "{indexName: index, query: {class: TermRangeQuery, field: body, lowerTerm: terma, upperTerm: termc, includeLower: false, includeUpper: false}, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
  }

  public void testMatchAllDocsQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{indexName: index, fields: {body: 'terma'}}");
    send("addDocument", "{indexName: index, fields: {body: 'termb'}}");
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'termc'}}"), "indexGen");
    assertEquals(3, getInt(send("search", "{indexName: index, query: {class: MatchAllDocsQuery}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testWildcardQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{indexName: index, fields: {body: 'terma'}}");
    send("addDocument", "{indexName: index, fields: {body: 'termb'}}");
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'termc'}}"), "indexGen");
    assertEquals(3, getInt(send("search", "{indexName: index, query: {class: WildcardQuery, field: body, term: 'term?'}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testFuzzyQuery() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'fantastic'}}"), "indexGen");
    assertEquals(1, getInt(send("search", "{indexName: index, query: {class: FuzzyQuery, field: body, term: 'fantasic', maxEdits: 1}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{indexName: index, query: {class: FuzzyQuery, field: body, term: 'fantasic', maxEdits: 2}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{indexName: index, query: {class: FuzzyQuery, field: body, term: 'fantasc', maxEdits: 1}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{indexName: index, query: {class: FuzzyQuery, field: body, term: 'fantasc', maxEdits: 2}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{indexName: index, query: {class: FuzzyQuery, field: body, term: 'fantasc', maxEdits: 2, prefixLength: 4}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testCommonTermsQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{indexName: index, fields: {body: 'fantastic'}}");
    send("addDocument", "{indexName: index, fields: {body: 'fantastic four'}}");
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'fantastic five'}}"), "indexGen");

    assertEquals(1, getInt(send("search", "{indexName: index, query: {class: CommonTermsQuery, highFreqOccur: must, lowFreqOccur: must, maxTermFrequency: 0.5, field: body, terms: [fantastic, four]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testMultiPhraseQuery() throws Exception {
    deleteAllDocs();
    send("addDocument", "{indexName: index, fields: {body: 'fantastic five is furious'}}");
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'fantastic four is furious'}}"), "indexGen");

    assertEquals(1, getInt(send("search", "{indexName: index, query: {class: MultiPhraseQuery, field: body, terms: [fantastic, five, is, furious]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(2, getInt(send("search", "{indexName: index, query: {class: MultiPhraseQuery, field: body, terms: [fantastic, {term: furious, position: 3}]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(2, getInt(send("search", "{indexName: index, query: {class: MultiPhraseQuery, field: body, terms: [fantastic, [five, four], {term: furious, position: 3}]}, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testClassicQPDefaultOperator() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'fantastic four is furious'}}"), "indexGen");
    
    assertEquals(1, getInt(send("search", "{indexName: index, queryParser: {class: classic, defaultOperator: or, defaultField: body}, queryText: 'furious five', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(0, getInt(send("search", "{indexName: index, queryParser: {class: classic, defaultOperator: and, defaultField: body}, queryText: 'furious five', searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testMultiFieldQP() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'fantastic four is furious', title: 'here is the title'}}"), "indexGen");
    
    assertEquals(1, getInt(send("search", "{indexName: index, queryParser: {class: MultiFieldQueryParser, defaultOperator: or, fields: [body, {field: title, boost: 2.0}]}, queryText: 'title furious', searcher: {indexGen: " + gen + "}}"), "totalHits"));
    assertEquals(1, getInt(send("search", "{indexName: index, queryParser: {class: MultiFieldQueryParser, defaultOperator: and, fields: [body, {field: title, boost: 2.0}]}, queryText: 'title furious', searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }

  public void testNumericRangeQuery() throws Exception {
    for(String type : new String[] {"int", "long", "float", "double"}) {
      send("createIndex", "{indexName: nrq, rootDir: nrq}");
      send("startIndex", "{indexName: nrq}");
      send("registerFields", String.format(Locale.ROOT, "{indexName: nrq, fields: {nf: {type: %s, index: true}}}", type));
      send("addDocument", "{indexName: nrq, fields: {nf: 5}}");
      send("addDocument", "{indexName: nrq, fields: {nf: 10}}");
      long gen = getLong(send("addDocument", "{indexName: nrq, fields: {nf: 17}}"), "indexGen");

      // Both min & max:
      assertEquals(3, getInt(send("search",

                                  String.format(Locale.ROOT, "{indexName: nrq, query: {class: NumericRangeQuery, field: nf, min: 5, max: 17, minInclusive: true, maxInclusive: true}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave min out:
      assertEquals(3, getInt(send("search",
                                  String.format(Locale.ROOT, "{indexName: nrq, query: {class: NumericRangeQuery, field: nf, max: 17, maxInclusive: true}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave min out, don't include max:
      assertEquals(2, getInt(send("search",
                                  String.format(Locale.ROOT, "{indexName: nrq, query: {class: NumericRangeQuery, field: nf, max: 17, maxInclusive: false}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave max out:
      assertEquals(3, getInt(send("search",
                                  String.format(Locale.ROOT, "{indexName: nrq, query: {class: NumericRangeQuery, field: nf, min: 5, minInclusive: true}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));

      // Leave max out, don't include max:
      assertEquals(2, getInt(send("search",
                                  String.format(Locale.ROOT, "{indexName: nrq, query: {class: NumericRangeQuery, field: nf, min: 5, minInclusive: false}, searcher: {indexGen: %d}}", gen)),
                             "totalHits"));
      send("stopIndex", "{indexName: nrq}");
      send("deleteIndex", "{indexName: nrq}");
    }
  }

  public void testSearchAfter() throws Exception {
    deleteAllDocs();
    long gen = 0;
    for(int i=0;i<20;i++) {
      gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'this is the body', id: " + i + "}}"), "indexGen");
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

      lastPage = send("search", "{indexName: index, query: MatchAllDocsQuery, topHits: 5, retrieveFields: [id], searcher: {indexGen: " + gen + "}" + sa + "}");
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

  public void testSearchAfterWithSort() throws Exception {
    deleteAllDocs();
    long gen = 0;
    for(int i=0;i<20;i++) {
      gen = getLong(send("addDocument", "{indexName: index, fields: {body: 'this is the body', id: " + i + "}}"), "indexGen");
    }

    JSONObject lastPage = null;

    Set<Integer> seenIDs = new HashSet<Integer>();

    // Pull 4 pages with 5 hits per page:
    for(int i=0;i<4;i++) {
      String sa;
      JSONObject o = new JSONObject();
      o.put("indexName", "index");
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
    _TestUtil.rmDir(new File("recency"));
    send("createIndex", "{indexName: recency, rootDir: recency}");
    send("startIndex", "{indexName: recency}");
    send("registerFields", "{indexName: recency, fields: {timestamp: {type: long, index: false, sort: true}, body: {type: text, analyzer: StandardAnalyzer}, blend: {type: virtual, recencyScoreBlend: {timeStampField: timestamp, maxBoost: 2.0, range: 30}}}}");

    long t = System.currentTimeMillis()/1000;
    send("addDocument", "{indexName: recency, fields: {body: 'this is some text', timestamp: " + (t-100) + "}}");
    long gen = getLong(send("addDocument", "{indexName: recency, fields: {body: 'this is some text', timestamp: " + t + "}}"), "indexGen");

    for(int pass=0;pass<2;pass++) {
      // Unboosted:
      JSONObject result = send("search", "{indexName: recency, queryText: text, searcher: {indexGen: " + gen + "}}");
      assertEquals(2, getInt(result, "totalHits"));
      assertEquals(0, getInt(result, "hits[0].doc"));
      assertEquals(1, getInt(result, "hits[1].doc"));

      // Relevance + recency changes the order:
      result = send("search", "{indexName: recency, queryText: text, sort: {fields: [{field: blend}]}, searcher: {indexGen: " + gen + "}}");
      assertEquals(2, getInt(result, "totalHits"));
      assertEquals(1, getInt(result, "hits[0].doc"));
      assertEquals(0, getInt(result, "hits[1].doc"));

      // Make sure this survives restart:
      send("stopIndex", "{indexName: recency}");
      send("startIndex", "{indexName: recency}");
    }
  }

  // nocommit test grouping
}
