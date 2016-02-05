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
package org.apache.lucene.queryparser.complexPhrase;

import java.util.HashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestComplexPhraseQuery extends LuceneTestCase {
  Directory rd;
  Analyzer analyzer;
  DocData docsContent[] = {
      new DocData("john smith", "1", "developer"),
      new DocData("johathon smith", "2", "developer"),
      new DocData("john percival smith", "3", "designer"),
      new DocData("jackson waits tom", "4", "project manager")
  };

  private IndexSearcher searcher;
  private IndexReader reader;

  String defaultFieldName = "name";

  boolean inOrder = true;

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


  public void testUnOrderedProximitySearches() throws Exception {

    inOrder = true;
    checkMatches("\"smith jo*\"~2", ""); // ordered proximity produces empty set

    inOrder = false;
    checkMatches("\"smith jo*\"~2", "1,2,3"); // un-ordered proximity

  }

  private void checkBadQuery(String qString) {
    ComplexPhraseQueryParser qp = new ComplexPhraseQueryParser(defaultFieldName, analyzer);
    qp.setInOrder(inOrder);
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
    ComplexPhraseQueryParser qp = new ComplexPhraseQueryParser(defaultFieldName, analyzer);
    qp.setInOrder(inOrder);
    qp.setFuzzyPrefixLength(1); // usually a good idea

    Query q = qp.parse(qString);

    HashSet<String> expecteds = new HashSet<>();
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
  
  public void testFieldedQuery() throws Exception {
    checkMatches("name:\"john smith\"", "1");
    checkMatches("name:\"j*   smyth~\"", "1,2");
    checkMatches("role:\"developer\"", "1,2");
    checkMatches("role:\"p* manager\"", "4");
    checkMatches("role:de*", "1,2,3");
    checkMatches("name:\"j* smyth~\"~5", "1,2,3");
    checkMatches("role:\"p* manager\" AND name:jack*", "4");
    checkMatches("+role:developer +name:jack*", "");
    checkMatches("name:\"john smith\"~2 AND role:designer AND id:3", "3");
  }

  public void testToStringContainsSlop() throws Exception {
    ComplexPhraseQueryParser qp = new ComplexPhraseQueryParser(defaultFieldName, analyzer);
    int slop = random().nextInt(31) + 1;

    String qString = "name:\"j* smyth~\"~" + slop;
    Query query = qp.parse(qString);
    assertTrue("Slop is not shown in toString()", query.toString().endsWith("~" + slop));

    String string = "\"j* smyth~\"";
    Query q = qp.parse(string);
    assertEquals("Don't show implicit slop of zero", q.toString(), string);
  }

  public void testHashcodeEquals() throws Exception {
    ComplexPhraseQueryParser qp = new ComplexPhraseQueryParser(defaultFieldName, analyzer);
    qp.setInOrder(true);
    qp.setFuzzyPrefixLength(1);

    String qString = "\"aaa* bbb*\"";

    Query q = qp.parse(qString);
    Query q2 = qp.parse(qString);

    assertEquals(q.hashCode(), q2.hashCode());
    assertEquals(q, q2);

    qp.setInOrder(false); // SOLR-6011

    q2 = qp.parse(qString);

    // although the general contract of hashCode can't guarantee different values, if we only change one thing
    // about a single query, it normally should result in a different value (and will with the current
    // implementation in ComplexPhraseQuery)
    assertTrue(q.hashCode() != q2.hashCode());
    assertTrue(!q.equals(q2));
    assertTrue(!q2.equals(q));
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    analyzer = new MockAnalyzer(random());
    rd = newDirectory();
    IndexWriter w = new IndexWriter(rd, newIndexWriterConfig(analyzer));
    for (int i = 0; i < docsContent.length; i++) {
      Document doc = new Document();
      doc.add(newTextField("name", docsContent[i].name, Field.Store.YES));
      doc.add(newTextField("id", docsContent[i].id, Field.Store.YES));
      doc.add(newTextField("role", docsContent[i].role, Field.Store.YES));
      w.addDocument(doc);
    }
    w.close();
    reader = DirectoryReader.open(rd);
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    rd.close();
    super.tearDown();
  }

  static class DocData {
    String name;

    String id;
    
    String role;

    public DocData(String name, String id, String role) {
      super();
      this.name = name;
      this.id = id;
      this.role = role;
    }
  }

}
