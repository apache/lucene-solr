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

package org.apache.lucene.concordance;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.concordance.charoffsets.SimpleAnalyzerUtil;
import org.apache.lucene.store.Directory;
import org.junit.BeforeClass;

public class TestSimpleAnalyzerUtil extends ConcordanceTestBase {

  private static Analyzer defaultCharOffsetGapAnalyzer;

  private static Analyzer customCharOffsetGapAnalyzer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    defaultCharOffsetGapAnalyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET, 0, 1);
    //customCharOffsetGapAnalyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET, 50, 213);
    customCharOffsetGapAnalyzer = getAnalyzer(MockTokenFilter.EMPTY_STOPSET, 50, 213);
  }
  /*
  public void testDebug() throws Exception {
    String[] values = new String[]{
        "the quick brown fox jumped over the lazy dog",
        "the fast green toad slid under the slothful rabbit",
        "the happy blue wolverine devoured the lazy moose",  
        "the depressed purple aardvark the the the the the the the devoured the energetic komodo",
        "the exasperated lavender lion",
        "the excited orange tiger the the the the the",
        "the colorless green idea slept furiously the"
    };
    System.out.println(values[0].length());
    List<String[]> docs = new ArrayList<>();
    docs.add(values);
    
    Directory directory = getDirectory(defaultCharOffsetGapAnalyzer, docs);
    
    String joiner = " | ";
    int gap = defaultCharOffsetGapAnalyzer.getOffsetGap(FIELD);
    IndexReader reader = DirectoryReader.open(directory);
    Document d = reader.document(0);
    String[] fieldValues = d.getValues(FIELD);
    //69, 103
    assertEquals("basic", "", testSimple(42, 45, fieldValues, gap, joiner));
    reader.close();
    directory.close();
  }*/

  public void testHitInGaps() throws Exception {
    String[] values = new String[]{
        "abc",
        "def",
        "ghi",
        "jkl"
    };
    List<String[]> docs = new ArrayList<>();
    docs.add(values);

    Directory directory = getDirectory(customCharOffsetGapAnalyzer, docs);

    String joiner = " | ";
    int gap = customCharOffsetGapAnalyzer.getOffsetGap(FIELD);
    IndexReader reader = DirectoryReader.open(directory);
    Document d = reader.document(0);
    String[] fieldValues = d.getValues(FIELD);

    assertEquals("two negs", "", testSimple(-10, -1, fieldValues, gap, joiner));

    assertEquals("two way beyonds", "", testSimple(1000, 1020, fieldValues, gap, joiner));

    assertEquals("two in betweens", " | ", testSimple(100, 110, fieldValues, gap, joiner));


    assertEquals("one neg", "abc", testSimple(-20, 3, fieldValues, gap, joiner));
    assertEquals("end < start 1", "", testSimple(3, -20, fieldValues, gap, joiner));
    assertEquals("end < start 2", "", testSimple(3, 2, fieldValues, gap, joiner));
    assertEquals("end in between", "abc", testSimple(0, 50, fieldValues, gap, joiner));
    //TODO: these used to be "def"; need to fix
    assertEquals("start in between", " | def", testSimple(5, 219, fieldValues, gap, joiner));
    assertEquals("start in between and end in between1", " | def", testSimple(5, 300, fieldValues, gap, joiner));
    assertEquals("start in between and end in between2", " | def | ghi", testSimple(5, 600, fieldValues, gap, joiner));
    assertEquals("", "def | ghi | jkl", testSimple(216, 10000, fieldValues, gap, joiner));

    reader.close();
    directory.close();

  }

  public void testRandomWithNeedleOnGaps() throws Exception {
    try {
      executeNeedleTests(defaultCharOffsetGapAnalyzer);
      executeNeedleTests(customCharOffsetGapAnalyzer);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void executeNeedleTests(Analyzer analyzer) throws Exception {

    String needle = getNeedle(analyzer);
    int numFieldValues = 23;

    Directory directory = buildNeedleIndex(needle, analyzer, numFieldValues);

    IndexReader reader = DirectoryReader.open(directory);

    LeafReaderContext ctx = reader.leaves().get(0);
    LeafReader r = ctx.reader();

    PostingsEnum dpe = r.postings(new Term(FIELD, needle), PostingsEnum.ALL);
    int numTests = 0;
    try {
      while (dpe.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        int frq = dpe.freq();
        int advanced = 0;

        String[] fieldValues = r.document(dpe.docID()).getValues(FIELD);
        while (++advanced < frq) {
          dpe.nextPosition();
          String rebuilt = SimpleAnalyzerUtil.substringFromMultiValuedFields(dpe.startOffset(),
              dpe.endOffset(), fieldValues, analyzer.getOffsetGap(FIELD), " | ");
          assertEquals(needle, rebuilt);
          numTests++;
        }
      }
    } finally {
      reader.close();
      directory.close();
    }
    assertEquals("number of tests", numFieldValues - 1, numTests);
  }

  private String testSimple(int start, int end, String[] fieldValues, int gap, String joiner) {
    return SimpleAnalyzerUtil.substringFromMultiValuedFields(start, end, fieldValues, gap, joiner);
  }
}
