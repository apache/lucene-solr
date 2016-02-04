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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** 
 * Tests coord() computation by BooleanQuery
 */
public class TestBooleanCoord extends LuceneTestCase {
  static Directory dir;
  static DirectoryReader reader;
  static IndexSearcher searcher;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    
    // we only add two documents for testing:
    // the first document has 3 terms A,B,C (for positive matching). we test scores against this.
    // the second document has 3 negative terms 1,2,3 that exist in the segment (for non-null scorers)
    // to test terms that don't exist (null scorers), we use X,Y,Z
    
    Document doc = new Document();
    doc.add(new StringField("field", "A", Field.Store.NO));
    doc.add(new StringField("field", "B", Field.Store.NO));
    doc.add(new StringField("field", "C", Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new StringField("field", "1", Field.Store.NO));
    doc.add(new StringField("field", "2", Field.Store.NO));
    doc.add(new StringField("field", "3", Field.Store.NO));
    iw.addDocument(doc);

    iw.close();
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    // we set a similarity that just returns 1, the idea is to test coord
    searcher.setSimilarity(new Similarity() {
      @Override
      public float coord(int overlap, int maxOverlap) {
        // we use a rather bogus/complex coord, because today coord() can really return anything.
        // note in the case of overlap == maxOverlap == 1: BooleanWeight always applies 1, (see LUCENE-4300).
        return overlap / (float)(maxOverlap + 1);
      }

      @Override
      public long computeNorm(FieldInvertState state) {
        throw new AssertionError();
      }

      @Override
      public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
        return new SimWeight() {
          @Override
          public float getValueForNormalization() {
            return 1f;
          }

          @Override
          public void normalize(float queryNorm, float topLevelBoost) {}
        };
      }

      @Override
      public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
        return new SimScorer() {
          @Override
          public float score(int doc, float freq) {
            return 1;
          }

          @Override
          public float computeSlopFactor(int distance) {
            throw new AssertionError();
          }

          @Override
          public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
            throw new AssertionError();
          }
        };
      }
    });
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    searcher = null;
  }
  
  // disjunctions
  
  public void testDisjunction1TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    // LUCENE-4300: coord(1,1) is always treated as 1
    assertScore(1 * 1, bq.build());
  }
  
  public void testDisjunction2TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(2f + 1), bq.build());
  }
  
  public void testDisjunction1OutOf2() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(2f + 1), bq.build());
  }
  
  public void testDisjunction1OutOf2Missing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(2f + 1), bq.build());
  }
  
  public void testDisjunction1OutOf3() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testDisjunction1OutOf3MissingOne() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testDisjunction1OutOf3MissingTwo() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Y"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testDisjunction2OutOf3() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testDisjunction2OutOf3Missing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  // disjunctions with coord disabled
  
  public void testDisjunction1TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testDisjunction2TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testDisjunction1OutOf2CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testDisjunction1OutOf2MissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testDisjunction1OutOf3CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testDisjunction1OutOf3MissingOneCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testDisjunction1OutOf3MissingTwoCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Y"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testDisjunction2OutOf3CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testDisjunction2OutOf3MissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  // minShouldMatch
  public void testMinShouldMatch1TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    // LUCENE-4300: coord(1,1) is always treated as 1
    assertScore(1 * 1, bq.build());
  }
  
  public void testMinShouldMatchn2TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(2f + 1), bq.build());
  }
  
  public void testMinShouldMatch1OutOf2() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(2f + 1), bq.build());
  }
  
  public void testMinShouldMatch1OutOf2Missing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(2f + 1), bq.build());
  }
  
  public void testMinShouldMatch1OutOf3() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testMinShouldMatch1OutOf3MissingOne() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testMinShouldMatch1OutOf3MissingTwo() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Y"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testMinShouldMatch2OutOf3() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testMinShouldMatch2OutOf3Missing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testMinShouldMatch2OutOf4() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(4f + 1), bq.build());
  }
  
  public void testMinShouldMatch2OutOf4Missing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(4f + 1), bq.build());
  }
  
  // minShouldMatch with coord disabled
  
  public void testMinShouldMatch1TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMinShouldMatch2TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMinShouldMatch1OutOf2CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMinShouldMatch1OutOf2MissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMinShouldMatch1OutOf3CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMinShouldMatch1OutOf3MissingOneCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMinShouldMatch1OutOf3MissingTwoCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("Y"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMinShouldMatch2OutOf3CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMinShouldMatch2OutOf3MissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMinShouldMatch2OutOf4CoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMinShouldMatch2OutOf4MissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.SHOULD);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  // conjunctions
  
  public void testConjunction1TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    // LUCENE-4300: coord(1,1) is always treated as 1
    assertScore(1 * 1, bq.build());
  }
  
  public void testConjunction1TermMatches1Prohib() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.MUST_NOT);
    // LUCENE-4300: coord(1,1) is always treated as 1
    assertScore(1 * 1, bq.build());
  }
  
  public void testConjunction1TermMatches2Prohib() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.MUST_NOT);
    bq.add(term("2"), BooleanClause.Occur.MUST_NOT);
    // LUCENE-4300: coord(1,1) is always treated as 1
    assertScore(1 * 1, bq.build());
  }
  
  public void testConjunction2TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.MUST);
    assertScore(2 * 2/(2f + 1), bq.build());
  }

  public void testConjunction3TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.MUST);
    bq.add(term("C"), BooleanClause.Occur.MUST);
    assertScore(3 * 3/(3f + 1), bq.build());
  }
  
  // conjunctions coord disabled
  
  public void testConjunction1TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    assertScore(1, bq.build());
  }
  
  public void testConjunction1TermMatches1ProhibCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.MUST_NOT);
    assertScore(1, bq.build());
  }
  
  public void testConjunction1TermMatches2ProhibCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.MUST_NOT);
    bq.add(term("2"), BooleanClause.Occur.MUST_NOT);
    assertScore(1, bq.build());
  }
  
  public void testConjunction2TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.MUST);
    assertScore(2, bq.build());
  }
  
  // optional + mandatory mix
  public void testMix2TermMatches() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(2f + 1), bq.build());
  }
  
  public void testMixMatch1OutOfTwo() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(2f + 1), bq.build());
  }
  
  public void testMixMatch1OutOfTwoMissing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(2f + 1), bq.build());
  }
  
  public void testMixMatch1OutOfThree() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testMixMatch1OutOfThreeOneMissing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testMixMatch2OutOfThree() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testMixMatch2OutOfThreeMissing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testMix2TermMatchesCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMixMatch1OutOfTwoCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMixMatch1OutOfTwoMissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMixMatch1OutOfThreeCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("2"), BooleanClause.Occur.SHOULD);
    assertScore(1, bq.build());
  }
  
  public void testMixMatch1OutOfThreeOneMissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(1 * 1/(3f + 1), bq.build());
  }
  
  public void testMixMatch2OutOfThreeCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMixMatch2OutOfThreeMissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  // min should match + mandatory mix
  
  public void testMixMinShouldMatch2OutOfThree() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testMixMinShouldMatch2OutOfThreeMissing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2 * 2/(3f + 1), bq.build());
  }
  
  public void testMixMinShouldMatch3OutOfFour() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("C"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(3 * 3/(4f + 1), bq.build());
  }
  
  public void testMixMinShouldMatch3OutOfFourMissing() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("C"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(3 * 3/(4f + 1), bq.build());
  }
  
  public void testMixMinShouldMatch2OutOfThreeCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMixMinShouldMatch2OutOfThreeMissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(1);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(2, bq.build());
  }
  
  public void testMixMinShouldMatch3OutOfFourCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("C"), BooleanClause.Occur.SHOULD);
    bq.add(term("1"), BooleanClause.Occur.SHOULD);
    assertScore(3, bq.build());
  }
  
  public void testMixMinShouldMatch3OutOfFourMissingCoordDisabled() throws Exception {
    BooleanQuery.Builder bq =new BooleanQuery.Builder();
    bq.setDisableCoord(true);
    bq.setMinimumNumberShouldMatch(2);
    bq.add(term("A"), BooleanClause.Occur.MUST);
    bq.add(term("B"), BooleanClause.Occur.SHOULD);
    bq.add(term("C"), BooleanClause.Occur.SHOULD);
    bq.add(term("Z"), BooleanClause.Occur.SHOULD);
    assertScore(3, bq.build());
  }
  
  // nested cases, make sure conjunctions propagate scoring
  
  public void testConjunctionNested() throws Exception {
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(term("A"), BooleanClause.Occur.MUST);
    inner.add(term("B"), BooleanClause.Occur.MUST);
    BooleanQuery.Builder outer = new BooleanQuery.Builder();
    outer.add(inner.build(), BooleanClause.Occur.MUST);
    outer.add(term("C"), BooleanClause.Occur.MUST);
    float innerScore = (1 + 1) * 2/(2f+1);
    float outerScore = (innerScore + 1) * 2/(2f+1);
    assertScore(outerScore, outer.build());
  }
  
  public void testConjunctionNestedOuterCoordDisabled() throws Exception {
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(term("A"), BooleanClause.Occur.MUST);
    inner.add(term("B"), BooleanClause.Occur.MUST);
    BooleanQuery.Builder outer = new BooleanQuery.Builder();
    outer.setDisableCoord(true);
    outer.add(inner.build(), BooleanClause.Occur.MUST);
    outer.add(term("C"), BooleanClause.Occur.MUST);
    float innerScore = (1 + 1) * 2/(2f+1);
    float outerScore = (innerScore + 1);
    assertScore(outerScore, outer.build());
  }
  
  public void testConjunctionNestedInnerCoordDisabled() throws Exception {
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.setDisableCoord(true);
    inner.add(term("A"), BooleanClause.Occur.MUST);
    inner.add(term("B"), BooleanClause.Occur.MUST);
    BooleanQuery.Builder outer = new BooleanQuery.Builder();
    outer.add(inner.build(), BooleanClause.Occur.MUST);
    outer.add(term("C"), BooleanClause.Occur.MUST);
    float innerScore = (1 + 1);
    float outerScore = (innerScore + 1) * 2/(2f+1);
    assertScore(outerScore, outer.build());
  }
  
  public void testConjunctionNestedCoordDisabledEverywhere() throws Exception {
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.setDisableCoord(true);
    inner.add(term("A"), BooleanClause.Occur.MUST);
    inner.add(term("B"), BooleanClause.Occur.MUST);
    BooleanQuery.Builder outer = new BooleanQuery.Builder();
    outer.setDisableCoord(true);
    outer.add(inner.build(), BooleanClause.Occur.MUST);
    outer.add(term("C"), BooleanClause.Occur.MUST);
    float innerScore = (1 + 1);
    float outerScore = (innerScore + 1);
    assertScore(outerScore, outer.build());
  }
  
  public void testConjunctionNestedSingle() throws Exception {
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(term("A"), BooleanClause.Occur.MUST);
    inner.add(term("B"), BooleanClause.Occur.MUST);
    BooleanQuery.Builder outer = new BooleanQuery.Builder();
    outer.add(inner.build(), BooleanClause.Occur.MUST);
    float innerScore = (1 + 1) * 2/(2f+1);
    // LUCENE-4300: coord(1,1) is always treated as 1
    float outerScore = innerScore * 1;
    assertScore(outerScore, outer.build());
  }
  
  /** asserts score for our single matching good doc */
  private void assertScore(final float expected, Query query) throws Exception {
    // test in-order
    Weight weight = searcher.createNormalizedWeight(query, true);
    Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer.docID() == -1 || scorer.docID() == DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(expected, scorer.score(), 0.0001f);

    // test bulk scorer
    final AtomicBoolean seen = new AtomicBoolean(false);
    BulkScorer bulkScorer = weight.bulkScorer(reader.leaves().get(0));
    assertNotNull(bulkScorer);
    bulkScorer.score(new LeafCollector() {
      Scorer scorer;
      
      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
      }
      
      @Override
      public void collect(int doc) throws IOException {
        assertFalse(seen.get());
        assertEquals(0, doc);
        assertEquals(expected, scorer.score(), 0.0001f);
        seen.set(true);
      }
    }, null, 0, 1);
    assertTrue(seen.get());

    // test the explanation
    Explanation expl = weight.explain(reader.leaves().get(0), 0);
    assertEquals(expected, expl.getValue(), 0.0001f);
  }
  
  private Query term(String s) {
    return new TermQuery(new Term("field", s));
  }
}
