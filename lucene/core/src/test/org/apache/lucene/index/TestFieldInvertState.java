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

package org.apache.lucene.index;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestFieldInvertState extends LuceneTestCase {
  /**
   * Similarity holds onto the FieldInvertState for subsequent verification.
   */
  private static class NeverForgetsSimilarity extends Similarity {
    public FieldInvertState lastState;
    private final static NeverForgetsSimilarity INSTANCE = new NeverForgetsSimilarity();

    private NeverForgetsSimilarity() {
      // no
    }
    
    @Override
    public long computeNorm(FieldInvertState state) {
      this.lastState = state;
      return 1;
    }
    
    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setSimilarity(NeverForgetsSimilarity.INSTANCE);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    Field field = new Field("field",
                            new CannedTokenStream(new Token("a", 0, 1),
                                                  new Token("b", 2, 3),
                                                  new Token("c", 4, 5)),
                            TextField.TYPE_NOT_STORED);
    doc.add(field);
    w.addDocument(doc);
    FieldInvertState fis = NeverForgetsSimilarity.INSTANCE.lastState;
    assertEquals(1, fis.getMaxTermFrequency());
    assertEquals(3, fis.getUniqueTermCount());
    assertEquals(0, fis.getNumOverlap());
    assertEquals(3, fis.getLength());
    IOUtils.close(w, dir);
  }

  public void testRandom() throws Exception {
    int numUniqueTokens = TestUtil.nextInt(random(), 1, 25);
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setSimilarity(NeverForgetsSimilarity.INSTANCE);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();

    int numTokens = atLeast(10000);
    Token[] tokens = new Token[numTokens];
    Map<Character,Integer> counts = new HashMap<>();
    int numStacked = 0;
    int maxTermFreq = 0;
    int pos = -1;
    for (int i=0;i<numTokens;i++) {
      char tokenChar = (char) ('a' + random().nextInt(numUniqueTokens));
      Integer oldCount = counts.get(tokenChar);
      int newCount;
      if (oldCount == null) {
        newCount = 1;
      } else {
        newCount = 1 + oldCount;
      }
      counts.put(tokenChar, newCount);
      maxTermFreq = Math.max(maxTermFreq, newCount);
      
      Token token = new Token(Character.toString(tokenChar), 2*i, 2*i+1);
      
      if (i > 0 && random().nextInt(7) == 3) {
        token.setPositionIncrement(0);
        numStacked++;
      } else {
        pos++;
      }
      tokens[i] = token;
    }

    Field field = new Field("field",
                            new CannedTokenStream(tokens),
                            TextField.TYPE_NOT_STORED);
    doc.add(field);
    w.addDocument(doc);
    FieldInvertState fis = NeverForgetsSimilarity.INSTANCE.lastState;
    assertEquals(maxTermFreq, fis.getMaxTermFrequency());
    assertEquals(counts.size(), fis.getUniqueTermCount());
    assertEquals(numStacked, fis.getNumOverlap());
    assertEquals(numTokens, fis.getLength());
    assertEquals(pos, fis.getPosition());
    
    IOUtils.close(w, dir);
  }
}
