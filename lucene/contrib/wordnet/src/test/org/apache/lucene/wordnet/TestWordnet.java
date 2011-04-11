package org.apache.lucene.wordnet;

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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestWordnet extends LuceneTestCase {
  private IndexSearcher searcher;
  private Directory dir;
  
  String storePathName = new File(TEMP_DIR,"testLuceneWordnet").getAbsolutePath();
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // create a temporary synonym index
    File testFile = getDataFile("testSynonyms.txt");
    String commandLineArgs[] = { testFile.getAbsolutePath(), storePathName };
    _TestUtil.rmDir(new File(storePathName));
    
    try {
      Syns2Index.main(commandLineArgs);
    } catch (Throwable t) { throw new RuntimeException(t); }
    
    dir = newFSDirectory(new File(storePathName));
    searcher = new IndexSearcher(dir, true);
  }
  
  public void testExpansion() throws IOException {
    assertExpandsTo("woods", new String[] { "woods", "forest", "wood" });
  }
  
  public void testExpansionSingleQuote() throws IOException {
    assertExpandsTo("king", new String[] { "king", "baron" });
  }
  
  private void assertExpandsTo(String term, String expected[]) throws IOException {
    Query expandedQuery = SynExpand.expand(term, searcher, new 
        MockAnalyzer(random), "field", 1F);
    BooleanQuery expectedQuery = new BooleanQuery();
    for (String t : expected)
      expectedQuery.add(new TermQuery(new Term("field", t)), 
          BooleanClause.Occur.SHOULD);
    assertEquals(expectedQuery, expandedQuery);
  }

  @Override
  public void tearDown() throws Exception {
    if (searcher != null) {
      searcher.close();
    }
    if (dir != null) {
      dir.close();
    }
    rmDir(storePathName); // delete our temporary synonym index
    super.tearDown();
  }
  
  private void rmDir(String directory) {
    File dir = new File(directory);
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
    dir.delete();
  }
}
