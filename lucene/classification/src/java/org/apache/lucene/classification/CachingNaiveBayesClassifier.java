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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiFields;
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
  private ArrayList<BytesRef> cclasses = new ArrayList<>();
  // it's a term-inmap style map, where the inmap contains class-hit pairs to the
  // upper term
  private Map<String, Map<BytesRef, Integer>> termCClassHitCache = new HashMap<>();
  // the term frequency in classes
  private Map<BytesRef, Double> classTermFreq = new HashMap<>();
  private boolean justCachedTerms;
  private int docsWithClassSize;

  /**
   * Creates a new NaiveBayes classifier with inside caching. Note that you must
   * call {@link #train(org.apache.lucene.index.LeafReader, String, String, Analyzer) train()} before
   * you can classify any documents. If you want less memory usage you could
   * call {@link #reInitCache(int, boolean) reInitCache()}.
   */
  public CachingNaiveBayesClassifier() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(LeafReader leafReader, String textFieldName, String classFieldName, Analyzer analyzer) throws IOException {
    train(leafReader, textFieldName, classFieldName, analyzer, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(LeafReader leafReader, String textFieldName, String classFieldName, Analyzer analyzer, Query query) throws IOException {
    train(leafReader, new String[]{textFieldName}, classFieldName, analyzer, query);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(LeafReader leafReader, String[] textFieldNames, String classFieldName, Analyzer analyzer, Query query) throws IOException {
    super.train(leafReader, textFieldNames, classFieldName, analyzer, query);
    // building the cache
    reInitCache(0, true);
  }

  private List<ClassificationResult<BytesRef>> assignClassNormalizedList(String inputDocument) throws IOException {
    if (leafReader == null) {
      throw new IOException("You must first call Classifier#train");
    }

    String[] tokenizedDoc = tokenizeDoc(inputDocument);

    List<ClassificationResult<BytesRef>> dataList = calculateLogLikelihood(tokenizedDoc);

    // normalization
    // The values transforms to a 0-1 range
    ArrayList<ClassificationResult<BytesRef>> returnList = new ArrayList<>();
    if (!dataList.isEmpty()) {
      Collections.sort(dataList);
      // this is a negative number closest to 0 = a
      double smax = dataList.get(0).getScore();

      double sumLog = 0;
      // log(sum(exp(x_n-a)))
      for (ClassificationResult<BytesRef> cr : dataList) {
        // getScore-smax <=0 (both negative, smax is the smallest abs()
        sumLog += Math.exp(cr.getScore() - smax);
      }
      // loga=a+log(sum(exp(x_n-a))) = log(sum(exp(x_n)))
      double loga = smax;
      loga += Math.log(sumLog);

      // 1/sum*x = exp(log(x))*1/sum = exp(log(x)-log(sum))
      for (ClassificationResult<BytesRef> cr : dataList) {
        returnList.add(new ClassificationResult<>(cr.getAssignedClass(), Math.exp(cr.getScore() - loga)));
      }
    }

    return returnList;
  }

  private List<ClassificationResult<BytesRef>> calculateLogLikelihood(String[] tokenizedDoc) throws IOException {
    // initialize the return List
    ArrayList<ClassificationResult<BytesRef>> ret = new ArrayList<>();
    for (BytesRef cclass : cclasses) {
      ClassificationResult<BytesRef> cr = new ClassificationResult<>(cclass, 0d);
      ret.add(cr);
    }
    // for each word
    for (String word : tokenizedDoc) {
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
        for (ClassificationResult<BytesRef> cr : ret) {
          if (cr.getAssignedClass().equals(cclass)) {
            cr.setScore(cr.getScore() + Math.log(wordProbability));
            break;
          }
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
        // threadsafe and concurent write
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
      TermsEnum termsEnum = leafReader.terms(textFieldName).iterator();
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
    Terms terms = MultiFields.getTerms(leafReader, classFieldName);
    TermsEnum termsEnum = terms.iterator();
    while ((termsEnum.next()) != null) {
      cclasses.add(BytesRef.deepCopyOf(termsEnum.term()));
    }
    // fill the classTermFreq map
    for (BytesRef cclass : cclasses) {
      double avgNumberOfUniqueTerms = 0;
      for (String textFieldName : textFieldNames) {
        terms = MultiFields.getTerms(leafReader, textFieldName);
        long numPostings = terms.getSumDocFreq(); // number of term/doc pairs
        avgNumberOfUniqueTerms += numPostings / (double) terms.getDocCount();
      }
      int docsWithC = leafReader.docFreq(new Term(classFieldName, cclass));
      classTermFreq.put(cclass, avgNumberOfUniqueTerms * docsWithC);
    }
  }
}
