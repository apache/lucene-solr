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
package org.apache.lucene.search.suggest.document;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.suggest.document.TopSuggestDocs.SuggestScoreDoc;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestTopSuggestDocs extends LuceneTestCase {

  public void testMerge() throws Exception {
    TopSuggestDocs[] toMerge = new TopSuggestDocs[3];
    for (int i = 0; i < toMerge.length; i++) {
      SuggestScoreDoc[] scoreDocs = new SuggestScoreDoc[5];
      for (int sd = 0; sd < 5; sd++) {
        scoreDocs[sd] = new SuggestScoreDoc(i * 5 + sd, "key_" + sd, "context", 5 - sd);
      }
      toMerge[i] = new TopSuggestDocs(new TotalHits(5, TotalHits.Relation.EQUAL_TO), scoreDocs, true);
    }
    
    TopSuggestDocs merged = TopSuggestDocs.merge(5, toMerge);
    assertEquals(5, merged.totalHits.value);
    assertTrue(merged.isComplete);
    assertEquals(5f, merged.scoreLookupDocs()[0].score, Float.MIN_VALUE);
    assertEquals(5f, merged.scoreLookupDocs()[1].score, Float.MIN_VALUE);
    assertEquals(5f, merged.scoreLookupDocs()[2].score, Float.MIN_VALUE);
    assertEquals(4f, merged.scoreLookupDocs()[3].score, Float.MIN_VALUE);
    assertEquals(4f, merged.scoreLookupDocs()[4].score, Float.MIN_VALUE);
    
    // setting one of the inputs to incomplete should render the result incomplete
    int indexToChange = TestUtil.nextInt(random(), 0, toMerge.length - 1);
    toMerge[indexToChange] = new TopSuggestDocs(new TotalHits(5, TotalHits.Relation.EQUAL_TO),
        toMerge[indexToChange].scoreLookupDocs(), false);
    merged = TopSuggestDocs.merge(5, toMerge);
    assertEquals(5, merged.totalHits.value);
    assertFalse(merged.isComplete);
  }
}
