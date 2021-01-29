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
package org.apache.lucene.search.similarities;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Abstract class to do basic tests for a similarity.
 * NOTE: This test focuses on the similarity impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new Similarity that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given Similarity that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseSimilarityTestCase extends LuceneTestCase {

  static LeafReader READER;
  static Directory DIR;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // with norms
    DIR = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), DIR);
    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setOmitNorms(true);
    doc.add(newField("field", "value", fieldType));
    writer.addDocument(doc);
    READER = getOnlyLeafReader(writer.getReader());
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.close(READER, DIR);
    READER = null;
    DIR = null;
  }

  /**
   * Return a new similarity with all parameters randomized within valid ranges.
   */
  protected abstract Similarity getSimilarity(Random random);
  
  static final long MAXDOC_FORTESTING = 1L << 48;
  // must be at least MAXDOC_FORTESTING + Integer.MAX_VALUE
  static final long MAXTOKENS_FORTESTING = 1L << 49;

  /**
   * returns a random corpus that is at least possible given
   * the norm value for a single document.
   */
  static CollectionStatistics newCorpus(Random random, int norm) {
    // lower bound of tokens in the collection (you produced this norm somehow)
    final int lowerBound;
    if (norm == 0) {
      // norms are omitted, but there must have been at least one token to produce that norm
      lowerBound = 1;    
    } else {
      // minimum value that would decode to such a norm
      lowerBound = SmallFloat.byte4ToInt((byte) norm);
    }
    final long maxDoc;
    switch (random.nextInt(6)) {
      case 0:
        // 1 doc collection
        maxDoc = 1;
        break;
      case 1:
        // 2 doc collection
        maxDoc = 2;
        break;
      case 2:
        // tiny collection
        maxDoc = TestUtil.nextLong(random, 3, 16);
        break;
      case 3:
        // small collection
        maxDoc = TestUtil.nextLong(random, 16, 100000);
        break;
      case 4:
        // big collection
        maxDoc = TestUtil.nextLong(random, 100000, MAXDOC_FORTESTING);
        break;
      default:
        // yuge collection
        maxDoc = MAXDOC_FORTESTING;
        break;
    }
    final long docCount;
    switch (random.nextInt(3)) {
      case 0:
        // sparsest field
        docCount = 1;
        break;
      case 1:
        // sparse field
        docCount = TestUtil.nextLong(random, 1, maxDoc);
        break;
      default:
        // fully populated
        docCount = maxDoc;
        break;
    }
    // random docsize: but can't require docs to have > 2B tokens
    long upperBound;
    try {
      upperBound = Math.min(MAXTOKENS_FORTESTING, Math.multiplyExact(docCount, Integer.MAX_VALUE));
    } catch (ArithmeticException overflow) {
      upperBound = MAXTOKENS_FORTESTING;
    }
    final long sumDocFreq;
    switch (random.nextInt(3)) {
      case 0:
        // shortest possible docs
        sumDocFreq = docCount;
        break;
      case 1:
        // biggest possible docs
        sumDocFreq = upperBound + 1 - lowerBound;
        break;
      default:
        // random docsize
        sumDocFreq = TestUtil.nextLong(random, docCount, upperBound + 1 - lowerBound);
        break;
    }
    final long sumTotalTermFreq;
    switch (random.nextInt(4)) {
      case 0:
        // term frequencies were omitted
        sumTotalTermFreq = sumDocFreq;
        break;
      case 1:
        // no repetition of terms (except to satisfy this norm)
        sumTotalTermFreq = sumDocFreq - 1 + lowerBound;
        break;
      case 2:
        // maximum repetition of terms
        sumTotalTermFreq = upperBound;
        break;
      default:
        // random repetition
        assert sumDocFreq - 1 + lowerBound <= upperBound;
        sumTotalTermFreq = TestUtil.nextLong(random, sumDocFreq - 1 + lowerBound, upperBound);
        break;
    }
    return new CollectionStatistics("field", maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
  }
  
  private static final BytesRef TERM = new BytesRef("term");

  /**
   * returns new random term, that fits within the bounds of the corpus
   */
  static TermStatistics newTerm(Random random, CollectionStatistics corpus) {
    final long docFreq;
    switch (random.nextInt(3)) {
      case 0:
        // rare term
        docFreq = 1;
        break;
      case 1:
        // common term
        docFreq = corpus.docCount();
        break;
      default:
        // random specificity
        docFreq = TestUtil.nextLong(random, 1, corpus.docCount());
        break;
    }
    final long totalTermFreq;
    // can't require docs to have > 2B tokens
    long upperBound;
    try {
      upperBound = Math.min(corpus.sumTotalTermFreq(), Math.multiplyExact(docFreq, Integer.MAX_VALUE));
    } catch (ArithmeticException overflow) {
      upperBound = corpus.sumTotalTermFreq();
    }
    if (corpus.sumTotalTermFreq() == corpus.sumDocFreq()) {
      // omitTF
      totalTermFreq = docFreq;
    } else {
      switch (random.nextInt(3)) {
        case 0:
          // no repetition
          totalTermFreq = docFreq;
          break;
        case 1:
          // maximum repetition
          totalTermFreq = upperBound;
          break;
        default:
          // random repetition
          totalTermFreq = TestUtil.nextLong(random, docFreq, upperBound);
          break;
      }
    }
    return new TermStatistics(TERM, docFreq, totalTermFreq);
  }

  /**
   * Tests scoring across a bunch of random terms/corpora/frequencies for each possible document length.
   * It does the following checks:
   * <ul>
   *   <li>scores are non-negative and finite.
   *   <li>score matches the explanation exactly.
   *   <li>internal explanations calculations are sane (e.g. sum of: and so on actually compute sums)
   *   <li>scores don't decrease as term frequencies increase: e.g. score(freq=N + 1) &gt;= score(freq=N)
   *   <li>scores don't decrease as documents get shorter, e.g. score(len=M) &gt;= score(len=M+1)
   *   <li>scores don't decrease as terms get rarer, e.g. score(term=N) &gt;= score(term=N+1)
   *   <li>scoring works for floating point frequencies (e.g. sloppy phrase and span queries will work)
   *   <li>scoring works for reasonably large 64-bit statistic values (e.g. distributed search will work)
   *   <li>scoring works for reasonably large boost values (0 .. Integer.MAX_VALUE, e.g. query boosts will work)
   *   <li>scoring works for parameters randomized within valid ranges (see {@link #getSimilarity(Random)})
   * </ul>
   */
  public void testRandomScoring() throws Exception {
    Random random = random();
    final int iterations = atLeast(1);
    for (int i = 0; i < iterations; i++) {
      // pull a new similarity to switch up parameters
      Similarity similarity = getSimilarity(random);
      for (int j = 0; j < 3; j++) {
        // for each norm value...
        for (int k = 1; k < 256; k++) {
          CollectionStatistics corpus = newCorpus(random, k);
          for (int l = 0; l < 10; l++) {
            TermStatistics term = newTerm(random, corpus);
            final float freq;
            if (term.totalTermFreq() == term.docFreq()) {
              // omit TF
              freq = 1;
            } else if (term.docFreq() == 1) {
              // only one document, all the instances must be here.
              freq = Math.toIntExact(term.totalTermFreq());
            } else {
              // there is at least one other document, and those must have at least 1 instance each.
              int upperBound = Math.toIntExact(Math.min(term.totalTermFreq() - term.docFreq() + 1, Integer.MAX_VALUE));
              if (random.nextBoolean()) {
                // integer freq
                switch (random.nextInt(3)) {
                  case 0:
                    // smallest freq
                    freq = 1;
                    break;
                  case 1:
                    // largest freq
                    freq = upperBound;
                    break;
                  default:
                    // random freq
                    freq = TestUtil.nextInt(random, 1, upperBound);
                    break;
                }
              } else {
                // float freq
                float freqCandidate;
                switch (random.nextInt(2)) {
                  case 0:
                    // smallest freq
                    freqCandidate = Float.MIN_VALUE;
                    break;
                  default:
                    // random freq
                    freqCandidate = upperBound * random.nextFloat();
                    break;
                }
                // we need to be 2nd float value at a minimum, the pairwise test will check MIN_VALUE in this case.
                // this avoids testing frequencies of 0 which seem wrong to allow (we should enforce computeSlopFactor etc)
                if (freqCandidate <= Float.MIN_VALUE) {
                  freqCandidate = Math.nextUp(Float.MIN_VALUE);
                }
                freq = freqCandidate;
              }
            }
            // we just limit the test to "reasonable" boost values but don't enforce this anywhere.
            // too big, and you are asking for overflow. that's hard for a sim to enforce (but definitely possible)
            // for now, we just want to detect overflow where its a real bug/hazard in the computation with reasonable inputs.
            final float boost;
            switch (random.nextInt(5)) {
              case 0:
                // minimum value (not enforced)
                boost = 0F;
                break;
              case 1:
                // tiny value
                boost = Float.MIN_VALUE;
                break;
              case 2:
                // no-op value (sometimes treated special in explanations)
                boost = 1F;
                break;
              case 3:
                // maximum value (not enforceD)
                boost = Integer.MAX_VALUE;
                break;
              default:
                // random value
                boost = random.nextFloat() * Integer.MAX_VALUE;
                break;
            }
            doTestScoring(similarity, corpus, term, boost, freq, k);
          }
        }
      }
    }
  }
  
  /** runs for a single test case, so that if you hit a test failure you can write a reproducer just for that scenario */
  private static void doTestScoring(Similarity similarity, CollectionStatistics corpus, TermStatistics term, float boost, float freq, int norm) throws IOException {
    boolean success = false;
    SimScorer scorer = similarity.scorer(boost, corpus, term);
    try {
      float maxScore = scorer.score(Float.MAX_VALUE, 1);
      assertFalse("maxScore is NaN", Float.isNaN(maxScore));

      float score = scorer.score(freq, norm);
      // check that score isn't infinite or negative
      assertTrue("infinite/NaN score: " + score, Float.isFinite(score));
      if (!(similarity instanceof IndriDirichletSimilarity)) {
        assertTrue("negative score: " + score, score >= 0);
      }
      assertTrue("greater than maxScore: " + score + ">" + maxScore, score <= maxScore);
      // check explanation matches
      Explanation explanation = scorer.explain(Explanation.match(freq, "freq, occurrences of term within document"), norm);
      if (score != explanation.getValue().doubleValue()) {
        fail("expected: " + score + ", got: " + explanation);
      }
      if (rarely()) {
        CheckHits.verifyExplanation("<test query>", 0, score, true, explanation);
      }
      
      // check score(freq-1), given the same norm it should be <= score(freq) [scores non-decreasing for more term occurrences]
      final float prevFreq;
      if (random().nextBoolean() && freq == (int)freq && freq > 1 && term.docFreq() > 1) {
        // previous in integer space
        prevFreq = freq - 1;
      } else {
        // previous in float space (e.g. for sloppyPhrase)
        prevFreq = Math.nextDown(freq);
      }
      
      float prevScore = scorer.score(prevFreq, norm);
      // check that score isn't infinite or negative
      assertTrue(Float.isFinite(prevScore));
      if (!(similarity instanceof IndriDirichletSimilarity)) {
        assertTrue(prevScore >= 0);
      }
      // check explanation matches
      Explanation prevExplanation = scorer.explain(Explanation.match(prevFreq, "freq, occurrences of term within document"), norm);
      if (prevScore != prevExplanation.getValue().doubleValue()) {
        fail("expected: " + prevScore + ", got: " + prevExplanation);
      }
      if (rarely()) {
        CheckHits.verifyExplanation("test query (prevFreq)", 0, prevScore, true, prevExplanation);
      }

      if (prevScore > score) {
        System.out.println(prevExplanation);
        System.out.println(explanation);
        fail("score(" + prevFreq + ")=" + prevScore + " > score(" + freq + ")=" + score);
      }
      
      // check score(norm-1), given the same freq it should be >= score(norm) [scores non-decreasing as docs get shorter]
      if (norm > 1) {
        float prevNormScore = scorer.score(freq, norm - 1);
        // check that score isn't infinite or negative
        assertTrue(Float.isFinite(prevNormScore));
        if (!(similarity instanceof IndriDirichletSimilarity)) {
          assertTrue(prevNormScore >= 0);
        }
        // check explanation matches
        Explanation prevNormExplanation = scorer.explain(Explanation.match(freq, "freq, occurrences of term within document"), norm - 1);
        if (prevNormScore != prevNormExplanation.getValue().doubleValue()) {
          fail("expected: " + prevNormScore + ", got: " + prevNormExplanation);
        }
        if (rarely()) {
          CheckHits.verifyExplanation("test query (prevNorm)", 0, prevNormScore, true, prevNormExplanation);
        }
        if (prevNormScore < score) {
          System.out.println(prevNormExplanation);
          System.out.println(explanation);
          fail("score(" + freq + "," + (norm-1) + ")=" + prevNormScore + " < score(" + freq + "," + norm + ")=" + score);
        }
      }
      
      // check score(term-1), given the same freq/norm it should be >= score(term) [scores non-decreasing as terms get rarer]
      if (term.docFreq() > 1 && freq < term.totalTermFreq()) {
        TermStatistics prevTerm = new TermStatistics(term.term(), term.docFreq() - 1, term.totalTermFreq() - 1);
        SimScorer prevTermScorer = similarity.scorer(boost, corpus, term);
        float prevTermScore = prevTermScorer.score(freq, norm);
        // check that score isn't infinite or negative
        assertTrue(Float.isFinite(prevTermScore));
        if (!(similarity instanceof IndriDirichletSimilarity)) {
          assertTrue(prevTermScore >= 0);
        }
        // check explanation matches
        Explanation prevTermExplanation = prevTermScorer.explain(Explanation.match(freq, "freq, occurrences of term within document"), norm);
        if (prevTermScore != prevTermExplanation.getValue().doubleValue()) {
          fail("expected: " + prevTermScore + ", got: " + prevTermExplanation);
        }
        if (rarely()) {
          CheckHits.verifyExplanation("test query (prevTerm)", 0, prevTermScore, true, prevTermExplanation);
        }

        if (prevTermScore < score) {
          System.out.println(prevTermExplanation);
          System.out.println(explanation);
          fail("score(" + freq + "," + (prevTerm) + ")=" + prevTermScore + " < score(" + freq + "," + term + ")=" + score);
        }
      }
      
      success = true;
    } finally {
      if (!success) {
        System.out.println(similarity);
        System.out.println(corpus);
        System.out.println(term);
        if (norm == 0) {
          System.out.println("norms=omitted");
        } else {
          System.out.println("norm=" + norm + " (doc length ~ " + SmallFloat.byte4ToInt((byte) norm) + ")");
        }
        System.out.println("freq=" + freq);
      }
    }
  }  
}
