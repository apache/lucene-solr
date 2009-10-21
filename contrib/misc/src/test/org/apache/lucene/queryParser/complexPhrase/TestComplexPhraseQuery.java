package org.apache.lucene.queryParser.complexPhrase;

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

import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;

public class TestComplexPhraseQuery extends TestCase {

  Analyzer analyzer = new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_CURRENT);

  DocData docsContent[] = { new DocData("john smith", "1"),
      new DocData("johathon smith", "2"),
      new DocData("john percival smith", "3"),
      new DocData("jackson waits tom", "4") };

  private IndexSearcher searcher;

  String defaultFieldName = "name";

  public void testComplexPhrases() throws Exception {
    checkMatches("\"john smith\"", "1"); // Simple multi-term still works
    checkMatches("\"j*   smyth~\"", "1,2"); // wildcards and fuzzies are OK in
    // phrases
    checkMatches("\"(jo* -john)  smith\"", "2"); // boolean logic works
    checkMatches("\"jo*  smith\"~2", "1,2,3"); // position logic works.
    checkMatches("\"jo* [sma TO smZ]\" ", "1,2"); // range queries supported
    checkMatches("\"john\"", "1,3"); // Simple single-term still works
    checkMatches("\"(john OR johathon)  smith\"", "1,2"); // boolean logic with
    // brackets works.
    checkMatches("\"(jo* -john) smyth~\"", "2"); // boolean logic with
    // brackets works.

    // checkMatches("\"john -percival\"", "1"); // not logic doesn't work
    // currently :(.

    checkMatches("\"john  nosuchword*\"", ""); // phrases with clauses producing
    // empty sets

    checkBadQuery("\"jo*  id:1 smith\""); // mixing fields in a phrase is bad
    checkBadQuery("\"jo* \"smith\" \""); // phrases inside phrases is bad
  }

  private void checkBadQuery(String qString) {
    QueryParser qp = new ComplexPhraseQueryParser(defaultFieldName, analyzer);
    Throwable expected = null;
    try {
      qp.parse(qString);
    } catch (Throwable e) {
      expected = e;
    }
    assertNotNull("Expected parse error in " + qString, expected);

  }

  private void checkMatches(String qString, String expectedVals)
      throws Exception {
    QueryParser qp = new ComplexPhraseQueryParser(defaultFieldName, analyzer);
    qp.setFuzzyPrefixLength(1); // usually a good idea

    Query q = qp.parse(qString);

    HashSet expecteds = new HashSet();
    String[] vals = expectedVals.split(",");
    for (int i = 0; i < vals.length; i++) {
      if (vals[i].length() > 0)
        expecteds.add(vals[i]);
    }

    TopDocs td = searcher.search(q, 10);
    ScoreDoc[] sd = td.scoreDocs;
    for (int i = 0; i < sd.length; i++) {
      Document doc = searcher.doc(sd[i].doc);
      String id = doc.get("id");
      assertTrue(qString + "matched doc#" + id + " not expected", expecteds
          .contains(id));
      expecteds.remove(id);
    }

    assertEquals(qString + " missing some matches ", 0, expecteds.size());

  }

  protected void setUp() throws Exception {
    RAMDirectory rd = new RAMDirectory();
    IndexWriter w = new IndexWriter(rd, analyzer, MaxFieldLength.UNLIMITED);
    for (int i = 0; i < docsContent.length; i++) {
      Document doc = new Document();
      doc.add(new Field("name", docsContent[i].name, Field.Store.YES,
          Field.Index.ANALYZED));
      doc.add(new Field("id", docsContent[i].id, Field.Store.YES,
          Field.Index.ANALYZED));
      w.addDocument(doc);
    }
    w.close();
    searcher = new IndexSearcher(rd, true);
  }

  protected void tearDown() throws Exception {
    searcher.close();
  }

  static class DocData {
    String name;

    String id;

    public DocData(String name, String id) {
      super();
      this.name = name;
      this.id = id;
    }
  }

}
