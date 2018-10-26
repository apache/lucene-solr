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
package org.apache.lucene.classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.util.BytesRef;

/**
 * A simplistic Lucene based NaiveBayes classifier, with caching feature, see
 * <code>http://en.wikipedia.org/wiki/Naive_Bayes_classifier</code>
 * <p>
 * This is NOT an online classifier.
 *
 * @lucene.experimental
 */
public class CachingNaiveBayesClassifier extends SimpleNaiveBayesClassifier {
  //for caching classes this will be the classification class list
  private final ArrayList<BytesRef> cclasses = new ArrayList<>();
  // it's a term-inmap style map, where the inmap contains class-hit pairs to the
  // upper term
  private final Map<String, Map<BytesRef, Integer>> termCClassHitCache = new HashMap<>();
  // the term frequency in classes
  private final Map<BytesRef, Double> classTermFreq = new HashMap<>();
  private boolean justCachedTerms;
  private int docsWithClassSize;

  /**
   * Creates a new NaiveBayes classifier with inside caching. If you want less memory usage you could call
   * {@link #reInitCache(int, boolean) reInitCache()}.
   *
   * @param indexReader     the reader on the index to be used for classification
   * @param analyzer       an {@link Analyzer} used to analyze unseen text
   * @param query          a {@link Query} to eventually filter the docs used for training the classifier, or {@code null}
   *                       if all the indexed docs should be used
   * @param classFieldName the name of the field used as the output for the classifier
   * @param textFieldNames the name of the fields used as the inputs for the classifier
   */
  public CachingNaiveBayesClassifier(IndexReader indexReader, Analyzer analyzer, Query query, String classFieldName, String... textFieldNames) {
    super(indexReader, analyzer, query, classFieldName, textFieldNames);
    // building the cache
    try {
      reInitCache(0, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  protected List<ClassificationResult<BytesRef>> assignClassNormalizedList(String inputDocument) throws IOException {
    String[] tokenizedText = tokenize(inputDocument);

    List<ClassificationResult<BytesRef>> assignedClasses = calculateLogLikelihood(tokenizedText);

    // normalization
    // The values transforms to a 0-1 range
    ArrayList<ClassificationResult<BytesRef>> asignedClassesNorm = super.normClassificationResults(assignedClasses);
    return asignedClassesNorm;
  }

  private List<ClassificationResult<BytesRef>> calculateLogLikelihood(String[] tokenizedText) throws IOException {
    // initialize the return List
    ArrayList<ClassificationResult<BytesRef>> ret = new ArrayList<>();
    for (BytesRef cclass : cclasses) {
      ClassificationResult<BytesRef> cr = new ClassificationResult<>(cclass, 0d);
      ret.add(cr);
    }
    // for each word
    for (String word : tokenizedText) {
      // search with text:word for all class:c
      Map<BytesRef, Integer> hitsInClasses = getWordFreqForClassess(word);
      // for each class
      for (BytesRef cclass : cclasses) {
        Integer hitsI = hitsInClasses.get(cclass);
        // if the word is out of scope hitsI could be null
        int hits = 0;
        if (hitsI != null) {
          hits = hitsI;
        }
        // num : count the no of times the word appears in documents of class c(+1)
        double num = hits + 1; // +1 is added because of add 1 smoothing

        // den : for the whole dictionary, count the no of times a word appears in documents of class c (+|V|)
        double den = classTermFreq.get(cclass) + docsWithClassSize;

        // P(w|c) = num/den
        double wordProbability = num / den;

        // modify the value in the result list item
        int removeIdx = -1;
        int i = 0;
        for (ClassificationResult<BytesRef> cr : ret) {
          if (cr.getAssignedClass().equals(cclass)) {
            removeIdx = i;
            break;
          }
          i++;
        }

        if (removeIdx >= 0) {
          ClassificationResult<BytesRef> toRemove = ret.get(removeIdx);
          ret.add(new ClassificationResult<>(toRemove.getAssignedClass(), toRemove.getScore() + Math.log(wordProbability)));
          ret.remove(removeIdx);
        }

      }
    }

    // log(P(d|c)) = log(P(w1|c))+...+log(P(wn|c))
    return ret;
  }

  private Map<BytesRef, Integer> getWordFreqForClassess(String word) throws IOException {

    Map<BytesRef, Integer> insertPoint;
    insertPoint = termCClassHitCache.get(word);

    // if we get the answer from the cache
    if (insertPoint != null) {
      if (!insertPoint.isEmpty()) {
        return insertPoint;
      }
    }

    Map<BytesRef, Integer> searched = new ConcurrentHashMap<>();

    // if we dont get the answer, but it's relevant we must search it and insert to the cache
    if (insertPoint != null || !justCachedTerms) {
      for (BytesRef cclass : cclasses) {
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        BooleanQuery.Builder subQuery = new BooleanQuery.Builder();
        for (String textFieldName : textFieldNames) {
          subQuery.add(new BooleanClause(new TermQuery(new Term(textFieldName, word)), BooleanClause.Occur.SHOULD));
        }
        booleanQuery.add(new BooleanClause(subQuery.build(), BooleanClause.Occur.MUST));
        booleanQuery.add(new BooleanClause(new TermQuery(new Term(classFieldName, cclass)), BooleanClause.Occur.MUST));
        if (query != null) {
          booleanQuery.add(query, BooleanClause.Occur.MUST);
        }
        TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
        indexSearcher.search(booleanQuery.build(), totalHitCountCollector);

        int ret = totalHitCountCollector.getTotalHits();
        if (ret != 0) {
          searched.put(cclass, ret);
        }
      }
      if (insertPoint != null) {
        // threadsafe and concurrent write
        termCClassHitCache.put(word, searched);
      }
    }

    return searched;
  }


  /**
   * This function is building the frame of the cache. The cache is storing the
   * word occurrences to the memory after those searched once. This cache can
   * made 2-100x speedup in proper use, but can eat lot of memory. There is an
   * option to lower the memory consume, if a word have really low occurrence in
   * the index you could filter it out. The other parameter is switching between
   * the term searching, if it true, just the terms in the skeleton will be
   * searched, but if it false the terms whoes not in the cache will be searched
   * out too (but not cached).
   *
   * @param minTermOccurrenceInCache Lower cache size with higher value.
   * @param justCachedTerms          The switch for fully exclude low occurrence docs.
   * @throws IOException If there is a low-level I/O error.
   */
  public void reInitCache(int minTermOccurrenceInCache, boolean justCachedTerms) throws IOException {
    this.justCachedTerms = justCachedTerms;

    this.docsWithClassSize = countDocsWithClass();
    termCClassHitCache.clear();
    cclasses.clear();
    classTermFreq.clear();

    // build the cache for the word
    Map<String, Long> frequencyMap = new HashMap<>();
    for (String textFieldName : textFieldNames) {
      TermsEnum termsEnum = MultiTerms.getTerms(indexReader, textFieldName).iterator();
      while (termsEnum.next() != null) {
        BytesRef term = termsEnum.term();
        String termText = term.utf8ToString();
        long frequency = termsEnum.docFreq();
        Long lastfreq = frequencyMap.get(termText);
        if (lastfreq != null) frequency += lastfreq;
        frequencyMap.put(termText, frequency);
      }
    }
    for (Map.Entry<String, Long> entry : frequencyMap.entrySet()) {
      if (entry.getValue() > minTermOccurrenceInCache) {
        termCClassHitCache.put(entry.getKey(), new ConcurrentHashMap<BytesRef, Integer>());
      }
    }

    // fill the class list
    Terms terms = MultiTerms.getTerms(indexReader, classFieldName);
    TermsEnum termsEnum = terms.iterator();
    while ((termsEnum.next()) != null) {
      cclasses.add(BytesRef.deepCopyOf(termsEnum.term()));
    }
    // fill the classTermFreq map
    for (BytesRef cclass : cclasses) {
      double avgNumberOfUniqueTerms = 0;
      for (String textFieldName : textFieldNames) {
        terms = MultiTerms.getTerms(indexReader, textFieldName);
        long numPostings = terms.getSumDocFreq(); // number of term/doc pairs
        avgNumberOfUniqueTerms += numPostings / (double) terms.getDocCount();
      }
      int docsWithC = indexReader.docFreq(new Term(classFieldName, cclass));
      classTermFreq.put(cclass, avgNumberOfUniqueTerms * docsWithC);
    }
  }
}
