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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Create an index with terms from 0000-9999.
 * Generates random regexps according to simple patterns,
 * and validates the correct number of hits are returned.
 */
public class TestRegexpRandom extends LuceneTestCase {
  private Searcher searcher;
  private Random random;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    RAMDirectory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new MockAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
    
    Document doc = new Document();
    Field field = new Field("field", "", Field.Store.NO, Field.Index.ANALYZED);
    doc.add(field);
    
    NumberFormat df = new DecimalFormat("0000");
    for (int i = 0; i < 10000; i++) {
      field.setValue(df.format(i));
      writer.addDocument(doc);
    }
    
    writer.optimize();
    writer.close();
    searcher = new IndexSearcher(dir);
  }
  
  private char N() {
    return (char) (0x30 + random.nextInt(10));
  }
  
  private String fillPattern(String wildcardPattern) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < wildcardPattern.length(); i++) {
      switch(wildcardPattern.charAt(i)) {
        case 'N':
          sb.append(N());
          break;
        default:
          sb.append(wildcardPattern.charAt(i));
      }
    }
    return sb.toString();
  }
  
  private void assertPatternHits(String pattern, int numHits) throws Exception {
    Query wq = new RegexpQuery(new Term("field", fillPattern(pattern)));
    TopDocs docs = searcher.search(wq, 25);
    assertEquals("Incorrect hits for pattern: " + pattern, numHits, docs.totalHits);
  }

  @Override
  protected void tearDown() throws Exception {
    searcher.close();
    super.tearDown();
  }
  
  public void testRegexps() throws Exception {
    random = newRandom(System.nanoTime());
    for (int i = 0; i < 100*_TestUtil.getRandomMultiplier(); i++) {
      assertPatternHits("NNNN", 1);
      assertPatternHits(".NNN", 10);
      assertPatternHits("N.NN", 10);
      assertPatternHits("NN.N", 10);
      assertPatternHits("NNN.", 10);
    }
    
    for (int i = 0; i < 10*_TestUtil.getRandomMultiplier(); i++) {
      assertPatternHits(".{1,2}NN", 100);
      assertPatternHits("N.{1,2}N", 100);
      assertPatternHits("NN.{1,2}", 100);
      assertPatternHits(".{1,3}N", 1000);
      assertPatternHits("N.{1,3}", 1000);
      assertPatternHits(".{1,4}", 10000);
      
      assertPatternHits("NNN[3-7]", 5);
      assertPatternHits("NN[2-6][3-7]", 25);
      assertPatternHits("N[1-5][2-6][3-7]", 125);
      assertPatternHits("[0-4][3-7][4-8][5-9]", 625);
      assertPatternHits("[3-7][2-6][0-4]N", 125);
      assertPatternHits("[2-6][3-7]NN", 25);
      assertPatternHits("[3-7]NNN", 5);
      
      assertPatternHits("NNN.*", 10);
      assertPatternHits("NN.*", 100);
      assertPatternHits("N.*", 1000);
      assertPatternHits(".*", 10000);
      
      assertPatternHits(".*NNN", 10);
      assertPatternHits(".*NN", 100);
      assertPatternHits(".*N", 1000);
      
      assertPatternHits("N.*NN", 10);
      assertPatternHits("NN.*N", 10);
      
      // combo of ? and * operators
      assertPatternHits(".NN.*", 100);
      assertPatternHits("N.N.*", 100);
      assertPatternHits("NN..*", 100);
      assertPatternHits(".N..*", 1000);
      assertPatternHits("N...*", 1000);
      
      assertPatternHits(".*NN.", 100);
      assertPatternHits(".*N..", 1000);
      assertPatternHits(".*...", 10000);
      assertPatternHits(".*.N.", 1000);
      assertPatternHits(".*..N", 1000);
    }
  }
}
