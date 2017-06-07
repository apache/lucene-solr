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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity.SimWeight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

/**
 * Tests the {@link SimilarityBase}-based Similarities. Contains unit tests and 
 * integration tests for all Similarities and correctness tests for a select
 * few.
 * <p>This class maintains a list of
 * {@code SimilarityBase} subclasses. Each test case performs its test on all
 * items in the list. If a test case fails, the name of the Similarity that
 * caused the failure is returned as part of the assertion error message.</p>
 * <p>Unit testing is performed by constructing statistics manually and calling
 * the {@link SimilarityBase#score(BasicStats, float, float)} method of the
 * Similarities. The statistics represent corner cases of corpus distributions.
 * </p>
 * <p>For the integration tests, a small (8-document) collection is indexed. The
 * tests verify that for a specific query, all relevant documents are returned
 * in the correct order. The collection consists of two poems of English poet
 * <a href="http://en.wikipedia.org/wiki/William_blake">William Blake</a>.</p>
 * <p>Note: the list of Similarities is maintained by hand. If a new Similarity
 * is added to the {@code org.apache.lucene.search.similarities} package, the
 * list should be updated accordingly.</p>
 * <p>
 * In the correctness tests, the score is verified against the result of manual
 * computation. Since it would be impossible to test all Similarities
 * (e.g. all possible DFR combinations, all parameter values for LM), only 
 * the best performing setups in the original papers are verified.
 * </p>
 */
public class TestSimilarityBase extends LuceneTestCase {
  private static String FIELD_BODY = "body";
  private static String FIELD_ID = "id";
  /** The tolerance range for float equality. */
  private static float FLOAT_EPSILON = 1e-5f;
  /** The DFR basic models to test. */
  static BasicModel[] BASIC_MODELS = {
    new BasicModelBE(), new BasicModelD(), new BasicModelG(),
    new BasicModelIF(), new BasicModelIn(), new BasicModelIne(),
    new BasicModelP()
  };
  /** The DFR aftereffects to test. */
  static AfterEffect[] AFTER_EFFECTS = {
    new AfterEffectB(), new AfterEffectL(), new AfterEffect.NoAfterEffect()
  };
  /** The DFR normalizations to test. */
  static Normalization[] NORMALIZATIONS = {
    new NormalizationH1(), new NormalizationH2(), new NormalizationH3(),
    new NormalizationZ(), new Normalization.NoNormalization()
  };
  /** The distributions for IB. */
  static Distribution[] DISTRIBUTIONS = {
    new DistributionLL(), new DistributionSPL()
  };
  /** Lambdas for IB. */
  static Lambda[] LAMBDAS = {
    new LambdaDF(), new LambdaTTF()
  };
  /** Independence measures for DFI */
  static Independence[] INDEPENDENCE_MEASURES = {
    new IndependenceStandardized(), new IndependenceSaturated(), new IndependenceChiSquared()  
  };
  
  private IndexSearcher searcher;
  private Directory dir;
  private IndexReader reader;
  /** The list of similarities to test. */
  private List<SimilarityBase> sims;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    for (int i = 0; i < docs.length; i++) {
      Document d = new Document();
      FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setIndexOptions(IndexOptions.NONE);
      d.add(newField(FIELD_ID, Integer.toString(i), ft));
      d.add(newTextField(FIELD_BODY, docs[i], Field.Store.YES));
      writer.addDocument(d);
    }
    
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
    
    sims = new ArrayList<>();
    for (BasicModel basicModel : BASIC_MODELS) {
      for (AfterEffect afterEffect : AFTER_EFFECTS) {
        for (Normalization normalization : NORMALIZATIONS) {
          sims.add(new DFRSimilarity(basicModel, afterEffect, normalization));
        }
      }
    }
    for (Distribution distribution : DISTRIBUTIONS) {
      for (Lambda lambda : LAMBDAS) {
        for (Normalization normalization : NORMALIZATIONS) {
          sims.add(new IBSimilarity(distribution, lambda, normalization));
        }
      }
    }
    sims.add(new LMDirichletSimilarity());
    sims.add(new LMJelinekMercerSimilarity(0.1f));
    sims.add(new LMJelinekMercerSimilarity(0.7f));
    for (Independence independence : INDEPENDENCE_MEASURES) {
      sims.add(new DFISimilarity(independence));
    }
  }
  
  // ------------------------------- Unit tests --------------------------------
  
  /** The default number of documents in the unit tests. */
  private static int NUMBER_OF_DOCUMENTS = 100;
  /** The default total number of tokens in the field in the unit tests. */
  private static long NUMBER_OF_FIELD_TOKENS = 5000;
  /** The default average field length in the unit tests. */
  private static float AVG_FIELD_LENGTH = 50;
  /** The default document frequency in the unit tests. */
  private static int DOC_FREQ = 10;
  /**
   * The default total number of occurrences of this term across all documents
   * in the unit tests.
   */
  private static long TOTAL_TERM_FREQ = 70;
  
  /** The default tf in the unit tests. */
  private static float FREQ = 7;
  /** The default document length in the unit tests. */
  private static int DOC_LEN = 40;
  
  /** Creates the default statistics object that the specific tests modify. */
  private BasicStats createStats() {
    BasicStats stats = new BasicStats("spoof", 1f);
    stats.setNumberOfDocuments(NUMBER_OF_DOCUMENTS);
    stats.setNumberOfFieldTokens(NUMBER_OF_FIELD_TOKENS);
    stats.setAvgFieldLength(AVG_FIELD_LENGTH);
    stats.setDocFreq(DOC_FREQ);
    stats.setTotalTermFreq(TOTAL_TERM_FREQ);
    return stats;
  }
  
  private CollectionStatistics toCollectionStats(BasicStats stats) {
    return new CollectionStatistics(stats.field, stats.getNumberOfDocuments(), -1, stats.getNumberOfFieldTokens(), -1);
  }
  
  private TermStatistics toTermStats(BasicStats stats) {
    return new TermStatistics(new BytesRef("spoofyText"), stats.getDocFreq(), stats.getTotalTermFreq());
  }
  /**
   * The generic test core called by all unit test methods. It calls the
   * {@link SimilarityBase#score(BasicStats, float, float)} method of all
   * Similarities in {@link #sims} and checks if the score is valid; i.e. it
   * is a finite positive real number.
   */
  private void unitTestCore(BasicStats stats, float freq, int docLen) {
    for (SimilarityBase sim : sims) {
      BasicStats realStats = (BasicStats) sim.computeWeight(
          stats.getBoost(),
          toCollectionStats(stats), 
          toTermStats(stats));
      float score = sim.score(realStats, freq, docLen);
      float explScore = sim.explain(
          realStats, 1, Explanation.match(freq, "freq"), docLen).getValue();
      assertFalse("Score infinite: " + sim.toString(), Float.isInfinite(score));
      assertFalse("Score NaN: " + sim.toString(), Float.isNaN(score));
      assertTrue("Score negative: " + sim.toString(), score >= 0);
      assertEquals("score() and explain() return different values: "
          + sim.toString(), score, explScore, FLOAT_EPSILON);
    }
  }
  
  /** Runs the unit test with the default statistics. */
  public void testDefault() throws IOException {
    unitTestCore(createStats(), FREQ, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when
   * {@code numberOfDocuments = numberOfFieldTokens}.
   */
  public void testSparseDocuments() throws IOException {
    BasicStats stats = createStats();
    stats.setNumberOfFieldTokens(stats.getNumberOfDocuments());
    stats.setTotalTermFreq(stats.getDocFreq());
    stats.setAvgFieldLength(
        (float)stats.getNumberOfFieldTokens() / stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code numberOfDocuments > numberOfFieldTokens}.
   */
  public void testVerySparseDocuments() throws IOException {
    BasicStats stats = createStats();
    stats.setNumberOfFieldTokens(stats.getNumberOfDocuments() * 2 / 3);
    stats.setTotalTermFreq(stats.getDocFreq());
    stats.setAvgFieldLength(
        (float)stats.getNumberOfFieldTokens() / stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when
   * {@code NumberOfDocuments = 1}.
   */
  public void testOneDocument() throws IOException {
    BasicStats stats = createStats();
    stats.setNumberOfDocuments(1);
    stats.setNumberOfFieldTokens(DOC_LEN);
    stats.setAvgFieldLength(DOC_LEN);
    stats.setDocFreq(1);
    stats.setTotalTermFreq((int)FREQ);
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code docFreq = numberOfDocuments}.
   */
  public void testAllDocumentsRelevant() throws IOException {
    BasicStats stats = createStats();
    float mult = (0.0f + stats.getNumberOfDocuments()) / stats.getDocFreq();
    stats.setTotalTermFreq((int)(stats.getTotalTermFreq() * mult));
    stats.setDocFreq(stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code docFreq > numberOfDocuments / 2}.
   */
  public void testMostDocumentsRelevant() throws IOException {
    BasicStats stats = createStats();
    float mult = (0.6f * stats.getNumberOfDocuments()) / stats.getDocFreq();
    stats.setTotalTermFreq((int)(stats.getTotalTermFreq() * mult));
    stats.setDocFreq((int)(stats.getNumberOfDocuments() * 0.6));
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code docFreq = 1}.
   */
  public void testOnlyOneRelevantDocument() throws IOException {
    BasicStats stats = createStats();
    stats.setDocFreq(1);
    stats.setTotalTermFreq((int)FREQ + 3);
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code totalTermFreq = numberOfFieldTokens}.
   */
  public void testAllTermsRelevant() throws IOException {
    BasicStats stats = createStats();
    stats.setTotalTermFreq(stats.getNumberOfFieldTokens());
    unitTestCore(stats, DOC_LEN, DOC_LEN);
    stats.setAvgFieldLength(DOC_LEN + 10);
    unitTestCore(stats, DOC_LEN, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code totalTermFreq > numberOfDocuments}.
   */
  public void testMoreTermsThanDocuments() throws IOException {
    BasicStats stats = createStats();
    stats.setTotalTermFreq(
        stats.getTotalTermFreq() + stats.getNumberOfDocuments());
    unitTestCore(stats, 2 * FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code totalTermFreq = numberOfDocuments}.
   */
  public void testNumberOfTermsAsDocuments() throws IOException {
    BasicStats stats = createStats();
    stats.setTotalTermFreq(stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when {@code totalTermFreq = 1}.
   */
  public void testOneTerm() throws IOException {
    BasicStats stats = createStats();
    stats.setDocFreq(1);
    stats.setTotalTermFreq(1);
    unitTestCore(stats, 1, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when {@code totalTermFreq = freq}.
   */
  public void testOneRelevantDocument() throws IOException {
    BasicStats stats = createStats();
    stats.setDocFreq(1);
    stats.setTotalTermFreq((int)FREQ);
    unitTestCore(stats, FREQ, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when {@code numberOfFieldTokens = freq}.
   */
  public void testAllTermsRelevantOnlyOneDocument() throws IOException {
    BasicStats stats = createStats();
    stats.setNumberOfDocuments(10);
    stats.setNumberOfFieldTokens(50);
    stats.setAvgFieldLength(5);
    stats.setDocFreq(1);
    stats.setTotalTermFreq(50);
    unitTestCore(stats, 50, 50);
  }

  /**
   * Tests correct behavior when there is only one document with a single term 
   * in the collection.
   */
  public void testOnlyOneTermOneDocument() throws IOException {
    BasicStats stats = createStats();
    stats.setNumberOfDocuments(1);
    stats.setNumberOfFieldTokens(1);
    stats.setAvgFieldLength(1);
    stats.setDocFreq(1);
    stats.setTotalTermFreq(1);
    unitTestCore(stats, 1, 1);
  }

  /**
   * Tests correct behavior when there is only one term in the field, but
   * more than one documents.
   */
  public void testOnlyOneTerm() throws IOException {
    BasicStats stats = createStats();
    stats.setNumberOfFieldTokens(1);
    stats.setAvgFieldLength(1.0f / stats.getNumberOfDocuments());
    stats.setDocFreq(1);
    stats.setTotalTermFreq(1);
    unitTestCore(stats, 1, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when {@code avgFieldLength = docLen}.
   */
  public void testDocumentLengthAverage() throws IOException {
    BasicStats stats = createStats();
    unitTestCore(stats, FREQ, (int)stats.getAvgFieldLength());
  }
  
  // ---------------------------- Correctness tests ----------------------------
  
  /** Correctness test for the Dirichlet LM model. */
  public void testLMDirichlet() throws IOException {
    float p =
        (FREQ + 2000.0f * (TOTAL_TERM_FREQ + 1) / (NUMBER_OF_FIELD_TOKENS + 1.0f)) /
        (DOC_LEN + 2000.0f);
    float a = 2000.0f / (DOC_LEN + 2000.0f);
    float gold = (float)(
        Math.log(p / (a * (TOTAL_TERM_FREQ + 1) / (NUMBER_OF_FIELD_TOKENS + 1.0f))) +
        Math.log(a));
    correctnessTestCore(new LMDirichletSimilarity(), gold);
  }
  
  /** Correctness test for the Jelinek-Mercer LM model. */
  public void testLMJelinekMercer() throws IOException {
    float p = (1 - 0.1f) * FREQ / DOC_LEN +
              0.1f * (TOTAL_TERM_FREQ + 1) / (NUMBER_OF_FIELD_TOKENS + 1.0f);
    float gold = (float)(Math.log(
        p / (0.1f * (TOTAL_TERM_FREQ + 1) / (NUMBER_OF_FIELD_TOKENS + 1.0f))));
    correctnessTestCore(new LMJelinekMercerSimilarity(0.1f), gold);
  }
  
  /**
   * Correctness test for the LL IB model with DF-based lambda and
   * no normalization.
   */
  public void testLLForIB() throws IOException {
    SimilarityBase sim = new IBSimilarity(new DistributionLL(), new LambdaDF(), new Normalization.NoNormalization());
    correctnessTestCore(sim, 4.178574562072754f);
  }
  
  /**
   * Correctness test for the SPL IB model with TTF-based lambda and
   * no normalization.
   */
  public void testSPLForIB() throws IOException {
    SimilarityBase sim =
      new IBSimilarity(new DistributionSPL(), new LambdaTTF(), new Normalization.NoNormalization());
    correctnessTestCore(sim, 2.2387237548828125f);
  }
  
  /** Correctness test for the PL2 DFR model. */
  public void testPL2() throws IOException {
    SimilarityBase sim = new DFRSimilarity(
        new BasicModelP(), new AfterEffectL(), new NormalizationH2());
    float tfn = (float)(FREQ * SimilarityBase.log2(
        1 + AVG_FIELD_LENGTH / DOC_LEN));  // 8.1894750101
    float l = 1.0f / (tfn + 1.0f);         // 0.108820144666
    float lambda = (1.0f + TOTAL_TERM_FREQ) / (1f + NUMBER_OF_DOCUMENTS);  // 0.7029703
    float p = (float)(tfn * SimilarityBase.log2(tfn / lambda) +
              (lambda + 1 / (12 * tfn) - tfn) * SimilarityBase.log2(Math.E) +
              0.5 * SimilarityBase.log2(2 * Math.PI * tfn)); // 21.065619
    float gold = l * p;                    // 2.2923636
    correctnessTestCore(sim, gold);
  }

  /** Correctness test for the IneB2 DFR model. */
  public void testIneB2() throws IOException {
    SimilarityBase sim = new DFRSimilarity(
        new BasicModelIne(), new AfterEffectB(), new NormalizationH2());
    correctnessTestCore(sim, 5.747603416442871f);
  }
  
  /** Correctness test for the GL1 DFR model. */
  public void testGL1() throws IOException {
    SimilarityBase sim = new DFRSimilarity(
        new BasicModelG(), new AfterEffectL(), new NormalizationH1());
    correctnessTestCore(sim, 1.6390540599822998f);
  }
  
  /** Correctness test for the BEB1 DFR model. */
  public void testBEB1() throws IOException {
    SimilarityBase sim = new DFRSimilarity(
        new BasicModelBE(), new AfterEffectB(), new NormalizationH1());
    float tfn = FREQ * AVG_FIELD_LENGTH / DOC_LEN;  // 8.75
    float b = (TOTAL_TERM_FREQ + 1 + 1) / ((DOC_FREQ + 1) * (tfn + 1));  // 0.67132866
    double f = TOTAL_TERM_FREQ + 1 + tfn;
    double n = f + NUMBER_OF_DOCUMENTS;
    double n1 = n + f - 1;        // 258.5
    double m1 = n + f - tfn - 2;  // 248.75
    double n2 = f;                                      // 79.75
    double m2 = f - tfn;                                // 71.0
    float be = (float)(-SimilarityBase.log2(n - 1) -
               SimilarityBase.log2(Math.E) +                   // -8.924494472554715
               ((m1 + 0.5f) * SimilarityBase.log2(n1 / m1) +
                (n1 - m1) * SimilarityBase.log2(n1)) -         // 91.9620374903885
               ((m2 + 0.5f) * SimilarityBase.log2(n2 / m2) +
                (n2 - m2) * SimilarityBase.log2(n2)));         // 67.26544321004599
               // 15.7720995
    float gold = b * be;                                       // 10.588263
    correctnessTestCore(sim, gold);
  }

  /** Correctness test for the D DFR model (basic model only). */
  public void testD() throws IOException {
    SimilarityBase sim = new DFRSimilarity(new BasicModelD(), new AfterEffect.NoAfterEffect(), new Normalization.NoNormalization());
    double totalTermFreqNorm = TOTAL_TERM_FREQ + FREQ + 1;
    double p = 1.0 / (NUMBER_OF_DOCUMENTS + 1);                // 0.009900990099009901
    double phi = FREQ / totalTermFreqNorm;                       // 0.08974358974358974
    double D = phi * SimilarityBase.log2(phi / p) +            // 0.17498542370019005
              (1 - phi) * SimilarityBase.log2((1 - phi) / (1 - p));
    float gold = (float)(totalTermFreqNorm * D + 0.5 * SimilarityBase.log2(
                 1 + 2 * Math.PI * FREQ * (1 - phi)));         // 16.328257
    correctnessTestCore(sim, gold);
  }
  
  /** Correctness test for the In2 DFR model with no aftereffect. */
  public void testIn2() throws IOException {
    SimilarityBase sim = new DFRSimilarity(
        new BasicModelIn(), new AfterEffect.NoAfterEffect(), new NormalizationH2());
    float tfn = (float)(FREQ * SimilarityBase.log2(            // 8.1894750101
                1 + AVG_FIELD_LENGTH / DOC_LEN));
    float gold = (float)(tfn * SimilarityBase.log2(            // 26.7459577898
                 (NUMBER_OF_DOCUMENTS + 1) / (DOC_FREQ + 0.5)));
    correctnessTestCore(sim, gold);
  }
  
  /** Correctness test for the IFB DFR model with no normalization. */
  public void testIFB() throws IOException {
    SimilarityBase sim = new DFRSimilarity(
        new BasicModelIF(), new AfterEffectB(), new Normalization.NoNormalization());
    float B = (TOTAL_TERM_FREQ + 1 + 1) / ((DOC_FREQ + 1) * (FREQ + 1)); // 0.8875
    float IF = (float)(FREQ * SimilarityBase.log2(             // 8.97759389642
               1 + (NUMBER_OF_DOCUMENTS + 1) / (TOTAL_TERM_FREQ + 0.5)));
    float gold = B * IF;                                       // 7.96761458307
    correctnessTestCore(sim, gold);
  }
  
  /**
   * The generic test core called by all correctness test methods. It calls the
   * {@link SimilarityBase#score(BasicStats, float, float)} method of all
   * Similarities in {@link #sims} and compares the score against the manually
   * computed {@code gold}.
   */
  private void correctnessTestCore(SimilarityBase sim, float gold) {
    BasicStats stats = createStats();
    BasicStats realStats = (BasicStats) sim.computeWeight(
        stats.getBoost(),
        toCollectionStats(stats), 
        toTermStats(stats));
    float score = sim.score(realStats, FREQ, DOC_LEN);
    assertEquals(
        sim.toString() + " score not correct.", gold, score, FLOAT_EPSILON);
  }
  
  // ---------------------------- Integration tests ----------------------------

  /** The "collection" for the integration tests. */
  String[] docs = new String[] {
      "Tiger, tiger burning bright   In the forest of the night   What immortal hand or eye   Could frame thy fearful symmetry ?",
      "In what distant depths or skies   Burnt the fire of thine eyes ?   On what wings dare he aspire ?   What the hands the seize the fire ?",
      "And what shoulder and what art   Could twist the sinews of thy heart ?   And when thy heart began to beat What dread hand ? And what dread feet ?",
      "What the hammer? What the chain ?   In what furnace was thy brain ?   What the anvil ? And what dread grasp   Dare its deadly terrors clasp ?",
      "And when the stars threw down their spears   And water'd heaven with their tear   Did he smile his work to see ?   Did he, who made the lamb, made thee ?",
      "Tiger, tiger burning bright   In the forest of the night   What immortal hand or eye   Dare frame thy fearful symmetry ?",
      "Cruelty has a human heart   And jealousy a human face   Terror the human form divine   And Secrecy the human dress .",
      "The human dress is forg'd iron   The human form a fiery forge   The human face a furnace seal'd   The human heart its fiery gorge ."
  };
  
  /**
   * Tests whether all similarities return three documents for the query word
   * "heart".
   */
  public void testHeartList() throws IOException {
    Query q = new TermQuery(new Term(FIELD_BODY, "heart"));
    
    for (SimilarityBase sim : sims) {
      searcher.setSimilarity(sim);
      TopDocs topDocs = searcher.search(q, 1000);
      assertEquals("Failed: " + sim.toString(), 3, topDocs.totalHits);
    }
  }
  
  /** Test whether all similarities return document 3 before documents 7 and 8. */
  public void testHeartRanking() throws IOException {
    Query q = new TermQuery(new Term(FIELD_BODY, "heart"));
    
    for (SimilarityBase sim : sims) {
      searcher.setSimilarity(sim);
      TopDocs topDocs = searcher.search(q, 1000);
      assertEquals("Failed: " + sim.toString(), "2", reader.document(topDocs.scoreDocs[0].doc).get(FIELD_ID));
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  // LUCENE-5221
  public void testDiscountOverlapsBoost() throws IOException {
    BM25Similarity expected = new BM25Similarity();
    SimilarityBase actual = new DFRSimilarity(new BasicModelIne(), new AfterEffectB(), new NormalizationH2());
    expected.setDiscountOverlaps(false);
    actual.setDiscountOverlaps(false);
    FieldInvertState state = new FieldInvertState(Version.LATEST.major, "foo");
    state.setLength(5);
    state.setNumOverlap(2);
    assertEquals(expected.computeNorm(state), actual.computeNorm(state));
    expected.setDiscountOverlaps(true);
    actual.setDiscountOverlaps(true);
    assertEquals(expected.computeNorm(state), actual.computeNorm(state));
  }

  public void testLengthEncodingBackwardCompatibility() throws IOException {
    Similarity similarity = RandomPicks.randomFrom(random(), sims);
    for (int indexCreatedVersionMajor : new int[] { Version.LUCENE_6_0_0.major, Version.LATEST.major}) {
      for (int length : new int[] {1, 2, 4}) { // these length values are encoded accurately on both cases
        Directory dir = newDirectory();
        // set the version on the directory
        new SegmentInfos(indexCreatedVersionMajor).commit(dir);
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(similarity));
        Document doc = new Document();
        String value = IntStream.range(0, length).mapToObj(i -> "b").collect(Collectors.joining(" "));
        doc.add(new TextField("foo", value, Store.NO));
        w.addDocument(doc);
        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(similarity);
        Term term = new Term("foo", "b");
        TermContext context = TermContext.build(reader.getContext(), term);
        SimWeight simWeight = similarity.computeWeight(1f, searcher.collectionStatistics("foo"), searcher.termStatistics(term, context));
        SimilarityBase.BasicSimScorer simScorer = (SimilarityBase.BasicSimScorer) similarity.simScorer(simWeight, reader.leaves().get(0));
        float docLength = simScorer.getLengthValue(0);
        assertEquals(length, (int) docLength);
        
        w.close();
        reader.close();
        dir.close();
      }
    }
  }
}
