package org.apache.lucene.search;

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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Create an index with terms from 000-999.
 * Generates random regexps according to simple patterns,
 * and validates the correct number of hits are returned.
 */
public class TestRegexpRandom extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(_TestUtil.nextInt(random(), 50, 1000)));
    
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setOmitNorms(true);
    Field field = newField("field", "", customType);
    doc.add(field);
    
    NumberFormat df = new DecimalFormat("000", new DecimalFormatSymbols(Locale.ROOT));
    for (int i = 0; i < 1000; i++) {
      field.setStringValue(df.format(i));
      writer.addDocument(doc);
    }
    
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  private char N() {
    return (char) (0x30 + random().nextInt(10));
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
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void testRegexps() throws Exception {
    int num = atLeast(1);
    for (int i = 0; i < num; i++) {
      assertPatternHits("NNN", 1);
      assertPatternHits(".NN", 10);
      assertPatternHits("N.N", 10);
      assertPatternHits("NN.", 10);
    }
    
    for (int i = 0; i < num; i++) {
      assertPatternHits(".{1,2}N", 100);
      assertPatternHits("N.{1,2}", 100);
      assertPatternHits(".{1,3}", 1000);
      
      assertPatternHits("NN[3-7]", 5);
      assertPatternHits("N[2-6][3-7]", 25);
      assertPatternHits("[1-5][2-6][3-7]", 125);
      assertPatternHits("[0-4][3-7][4-8]", 125);
      assertPatternHits("[2-6][0-4]N", 25);
      assertPatternHits("[2-6]NN", 5);
      
      assertPatternHits("NN.*", 10);
      assertPatternHits("N.*", 100);
      assertPatternHits(".*", 1000);
      
      assertPatternHits(".*NN", 10);
      assertPatternHits(".*N", 100);
      
      assertPatternHits("N.*N", 10);
      
      // combo of ? and * operators
      assertPatternHits(".N.*", 100);
      assertPatternHits("N..*", 100);
      
      assertPatternHits(".*N.", 100);
      assertPatternHits(".*..", 1000);
      assertPatternHits(".*.N", 100);
    }
  }
}
