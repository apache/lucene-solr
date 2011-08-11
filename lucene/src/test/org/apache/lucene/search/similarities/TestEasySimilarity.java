package org.apache.lucene.search.similarities;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TermContext;

/**
 * Tests the {@link EasySimilarity}-based Similarities. Contains unit tests and
 * integration tests as well. This class maintains a list of
 * {@code EasySimilarity} subclasses. Each test case performs its test on all
 * items in the list. If a test case fails, the name of the Similarity that
 * caused the failure is returned as part of the assertion error message.
 * <p>Unit testing is performed by constructing statistics manually and calling
 * the {@link EasySimilarity#score(EasyStats, float, int)} method of the
 * Similarities. The statistics represent corner cases of corpus distributions.
 * </p>
 * <p>For the integration tests, a small (8-document) collection is indexed. The
 * tests verify that for a specific query, all relevant documents are returned
 * in the correct order. The collection consists of two poems of English poet
 * <a href="http://en.wikipedia.org/wiki/William_blake">William Blake</a>.</p>
 * <p>Note: the list of Similarities is maintained by hand. If a new Similarity
 * is added to the {@code org.apache.lucene.search.similarities} package, the
 * list should be updated accordingly.</p>
 */
public class TestEasySimilarity extends LuceneTestCase {
  private static String FIELD_BODY = "body";
  private static String FIELD_ID = "id";
  /** The DFR basic models to test. */
  private static BasicModel[] BASIC_MODELS;
  /** The DFR aftereffects to test. */
  private static AfterEffect[] AFTER_EFFECTS;
  /** The DFR normalizations to test. */
  private static Normalization[] NORMALIZATIONS;
  /** The distributions for IB. */
  private static Distribution[] DISTRIBUTIONS;
  /** Lambdas for IB. */
  private static Lambda[] LAMBDAS;
  
  static {
    BASIC_MODELS = new BasicModel[] {
        new BasicModelBE(), new BasicModelD(), new BasicModelG(),
        new BasicModelIF(), new BasicModelIn(), new BasicModelIne(),
        new BasicModelP()
    };
    AFTER_EFFECTS = new AfterEffect[] {
        new AfterEffectB(), new AfterEffectL(), new AfterEffect.NoAfterEffect()
    };
    NORMALIZATIONS = new Normalization[] {
        new NormalizationH1(), new NormalizationH2(),
        new Normalization.NoNormalization()
    };
    DISTRIBUTIONS = new Distribution[] {
        new DistributionLL(), new DistributionSPL()
    };
    LAMBDAS = new Lambda[] {
        new LambdaDF(), new LambdaTTF()
    };
  }
  
  private IndexSearcher searcher;
  private Directory dir;
  private IndexReader reader;
  /** The list of similarities to test. */
  private List<EasySimilarity> sims;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);

    for (int i = 0; i < docs.length; i++) {
      Document d = new Document();
      d.add(newField(FIELD_ID, Integer.toString(i), Field.Store.YES, Field.Index.NO));
      d.add(newField(FIELD_BODY, docs[i], Field.Index.ANALYZED));
      writer.addDocument(d);
    }
    
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
    
    sims = new ArrayList<EasySimilarity>();
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
  private EasyStats createStats() {
    EasyStats stats = new EasyStats(1);
    stats.setNumberOfDocuments(NUMBER_OF_DOCUMENTS);
    stats.setNumberOfFieldTokens(NUMBER_OF_FIELD_TOKENS);
    stats.setAvgFieldLength(AVG_FIELD_LENGTH);
    stats.setDocFreq(DOC_FREQ);
    stats.setTotalTermFreq(TOTAL_TERM_FREQ);
    return stats;
  }

  /**
   * The generic test core called by all unit test methods. It calls the
   * {@link EasySimilarity#score(EasyStats, float, int)} method of all
   * Similarities in {@link #sims} and checks if the score is valid; i.e. it
   * is a finite positive real number.
   */
  private void unitTestCore(EasyStats stats, float freq, int docLen)
      throws IOException {
    // We have to fake everything, because computeStats() can be overridden and
    // there is no way to inject false data after fillEasyStats().
    SpoofIndexSearcher searcher = new SpoofIndexSearcher(stats);
    TermContext tc = new TermContext(
        searcher.getIndexReader().getTopReaderContext(),
        new OrdTermState(), 0, stats.getDocFreq(), stats.getTotalTermFreq());
    
    for (EasySimilarity sim : sims) {
      EasyStats realStats = sim.computeStats(new SpoofIndexSearcher(stats),
          "spoof", stats.getTotalBoost(), tc);
//      System.out.printf("Before: %d %d %f %d %d%n",
//          realStats.getNumberOfDocuments(), realStats.getNumberOfFieldTokens(),
//          realStats.getAvgFieldLength(), realStats.getDocFreq(),
//          realStats.getTotalTermFreq());
//      realStats.setNumberOfDocuments(stats.getNumberOfDocuments());
//      realStats.setNumberOfFieldTokens(stats.getNumberOfFieldTokens());
//      realStats.setAvgFieldLength(stats.getAvgFieldLength());
//      realStats.setDocFreq(stats.getDocFreq());
//      realStats.setTotalTermFreq(stats.getTotalTermFreq());
//      System.out.printf("After: %d %d %f %d %d%n",
//          realStats.getNumberOfDocuments(), realStats.getNumberOfFieldTokens(),
//          realStats.getAvgFieldLength(), realStats.getDocFreq(),
//          realStats.getTotalTermFreq());
      float score = sim.score(realStats, freq, docLen);
      assertFalse("Score infinite: " + sim.toString(), Float.isInfinite(score));
      assertFalse("Score NaN: " + sim.toString(), Float.isNaN(score));
      assertTrue("Score negative: " + sim.toString(), score >= 0);
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
    EasyStats stats = createStats();
    stats.setNumberOfFieldTokens(stats.getNumberOfDocuments());
    stats.setTotalTermFreq(stats.getDocFreq());
    stats.setAvgFieldLength(
        stats.getNumberOfFieldTokens() / stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code numberOfDocuments > numberOfFieldTokens}.
   */
  public void testVerySparseDocuments() throws IOException {
    EasyStats stats = createStats();
    stats.setNumberOfFieldTokens(stats.getNumberOfDocuments() * 2 / 3);
    stats.setTotalTermFreq(stats.getDocFreq());
    stats.setAvgFieldLength(
        stats.getNumberOfFieldTokens() / stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code NumberOfDocuments = 1}.
   */
  public void testOneDocument() throws IOException {
    EasyStats stats = createStats();
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
    EasyStats stats = createStats();
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
    EasyStats stats = createStats();
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
    EasyStats stats = createStats();
    stats.setDocFreq(1);
    stats.setTotalTermFreq((int)FREQ + 3);
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code totalTermFreq = numberOfFieldTokens}.
   */
  public void testAllTermsRelevant() throws IOException {
    EasyStats stats = createStats();
    stats.setTotalTermFreq(stats.getNumberOfFieldTokens());
    unitTestCore(stats, DOC_LEN, DOC_LEN);
    // nocommit docLen > avglength
  }

  /**
   * Tests correct behavior when
   * {@code totalTermFreq > numberOfDocuments}.
   */
  public void testMoreTermsThanDocuments() throws IOException {
    EasyStats stats = createStats();
    stats.setTotalTermFreq(
        stats.getTotalTermFreq() + stats.getNumberOfDocuments());
    unitTestCore(stats, 2 * FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when
   * {@code totalTermFreq = numberOfDocuments}.
   */
  public void testNumberOfTermsAsDocuments() throws IOException {
    EasyStats stats = createStats();
    stats.setTotalTermFreq(stats.getNumberOfDocuments());
    unitTestCore(stats, FREQ, DOC_LEN);
  }

  /**
   * Tests correct behavior when {@code totalTermFreq = 1}.
   */
  public void testOneTerm() throws IOException {
    EasyStats stats = createStats();
    stats.setDocFreq(1);
    stats.setTotalTermFreq(1);
    unitTestCore(stats, 1, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when {@code totalTermFreq = freq}.
   */
  public void testOneRelevantDocument() throws IOException {
    EasyStats stats = createStats();
    stats.setDocFreq(1);
    stats.setTotalTermFreq((int)FREQ);
    unitTestCore(stats, FREQ, DOC_LEN);
  }
  
  /**
   * Tests correct behavior when {@code numberOfFieldTokens = freq}.
   */
  public void testAllTermsRelevantOnlyOneDocument() throws IOException {
    EasyStats stats = createStats();
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
    EasyStats stats = createStats();
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
    EasyStats stats = createStats();
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
    EasyStats stats = createStats();
    unitTestCore(stats, FREQ, (int)stats.getAvgFieldLength());
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
    
    for (EasySimilarity sim : sims) {
      searcher.setSimilarityProvider(new EasySimilarityProvider(sim));
      TopDocs topDocs = searcher.search(q, 1000);
      assertEquals("Failed: " + sim.toString(), 3, topDocs.totalHits);
    }
  }
  
  /** Test whether all similarities return document 3 before documents 7 and 8. */
  public void testHeartRanking() throws IOException {
    Query q = new TermQuery(new Term(FIELD_BODY, "heart"));
    
    for (EasySimilarity sim : sims) {
      searcher.setSimilarityProvider(new EasySimilarityProvider(sim));
      TopDocs topDocs = searcher.search(q, 1000);
      assertEquals("Failed: " + sim.toString(), 2, topDocs.scoreDocs[0].doc);
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    searcher.close();
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  // ------------------------- Helper class definitions ------------------------
  
  /**
   * A simple Similarity provider that returns in {@code get(String field)} the
   * object passed to its constructor.
   */
  // nocommit a real EasySimilarityProvider with coord and queryNorm implemented?
  public static class EasySimilarityProvider implements SimilarityProvider {
    private EasySimilarity sim;
    
    public EasySimilarityProvider(EasySimilarity sim) {
      this.sim = sim;
    }
    
    @Override
    public float coord(int overlap, int maxOverlap) {
      return 1f;
    }

    @Override
    public float queryNorm(float sumOfSquaredWeights) {
      return 1f;
    }

    @Override
    public EasySimilarity get(String field) {
      return sim;
    }
  }
}