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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * A simplistic Lucene based NaiveBayes classifier, see <code>http://en.wikipedia.org/wiki/Naive_Bayes_classifier</code>
 *
 * @lucene.experimental
 */
public class SimpleNaiveBayesClassifier implements Classifier<BytesRef> {

  private AtomicReader atomicReader;
  private String[] textFieldNames;
  private String classFieldName;
  private int docsWithClassSize;
  private Analyzer analyzer;
  private IndexSearcher indexSearcher;
  private Query query;

  /**
   * Creates a new NaiveBayes classifier.
   * Note that you must call {@link #train(AtomicReader, String, String, Analyzer) train()} before you can
   * classify any documents.
   */
  public SimpleNaiveBayesClassifier() {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(AtomicReader atomicReader, String textFieldName, String classFieldName, Analyzer analyzer) throws IOException {
    train(atomicReader, textFieldName, classFieldName, analyzer, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(AtomicReader atomicReader, String textFieldName, String classFieldName, Analyzer analyzer, Query query)
      throws IOException {
    train(atomicReader, new String[]{textFieldName}, classFieldName, analyzer, query);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(AtomicReader atomicReader, String[] textFieldNames, String classFieldName, Analyzer analyzer, Query query)
      throws IOException {
    this.atomicReader = atomicReader;
    this.indexSearcher = new IndexSearcher(this.atomicReader);
    this.textFieldNames = textFieldNames;
    this.classFieldName = classFieldName;
    this.analyzer = analyzer;
    this.query = query;
    this.docsWithClassSize = countDocsWithClass();
  }

  private int countDocsWithClass() throws IOException {
    int docCount = MultiFields.getTerms(this.atomicReader, this.classFieldName).getDocCount();
    if (docCount == -1) { // in case codec doesn't support getDocCount
      TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
      BooleanQuery q = new BooleanQuery();
      q.add(new BooleanClause(new WildcardQuery(new Term(classFieldName, String.valueOf(WildcardQuery.WILDCARD_STRING))), BooleanClause.Occur.MUST));
      if (query != null) {
        q.add(query, BooleanClause.Occur.MUST);
      }
      indexSearcher.search(q,
          totalHitCountCollector);
      docCount = totalHitCountCollector.getTotalHits();
    }
    return docCount;
  }

  private String[] tokenizeDoc(String doc) throws IOException {
    Collection<String> result = new LinkedList<>();
    for (String textFieldName : textFieldNames) {
      TokenStream tokenStream = analyzer.tokenStream(textFieldName, doc);
      try {
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
          result.add(charTermAttribute.toString());
        }
        tokenStream.end();
      } finally {
        IOUtils.closeWhileHandlingException(tokenStream);
      }
    }
    return result.toArray(new String[result.size()]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassificationResult<BytesRef> assignClass(String inputDocument) throws IOException {
    if (atomicReader == null) {
      throw new IOException("You must first call Classifier#train");
    }
    double max = - Double.MAX_VALUE;
    BytesRef foundClass = new BytesRef();

    Terms terms = MultiFields.getTerms(atomicReader, classFieldName);
    TermsEnum termsEnum = terms.iterator(null);
    BytesRef next;
    String[] tokenizedDoc = tokenizeDoc(inputDocument);
    while ((next = termsEnum.next()) != null) {
      double clVal = calculateLogPrior(next) + calculateLogLikelihood(tokenizedDoc, next);
      if (clVal > max) {
        max = clVal;
        foundClass = BytesRef.deepCopyOf(next);
      }
    }
    double score = 10 / Math.abs(max);
    return new ClassificationResult<>(foundClass, score);
  }


  private double calculateLogLikelihood(String[] tokenizedDoc, BytesRef c) throws IOException {
    // for each word
    double result = 0d;
    for (String word : tokenizedDoc) {
      // search with text:word AND class:c
      int hits = getWordFreqForClass(word, c);

      // num : count the no of times the word appears in documents of class c (+1)
      double num = hits + 1; // +1 is added because of add 1 smoothing

      // den : for the whole dictionary, count the no of times a word appears in documents of class c (+|V|)
      double den = getTextTermFreqForClass(c) + docsWithClassSize;

      // P(w|c) = num/den
      double wordProbability = num / den;
      result += Math.log(wordProbability);
    }

    // log(P(d|c)) = log(P(w1|c))+...+log(P(wn|c))
    return result;
  }

  private double getTextTermFreqForClass(BytesRef c) throws IOException {
    double avgNumberOfUniqueTerms = 0;
    for (String textFieldName : textFieldNames) {
      Terms terms = MultiFields.getTerms(atomicReader, textFieldName);
      long numPostings = terms.getSumDocFreq(); // number of term/doc pairs
      avgNumberOfUniqueTerms += numPostings / (double) terms.getDocCount(); // avg # of unique terms per doc
    }
    int docsWithC = atomicReader.docFreq(new Term(classFieldName, c));
    return avgNumberOfUniqueTerms * docsWithC; // avg # of unique terms in text fields per doc * # docs with c
  }

  private int getWordFreqForClass(String word, BytesRef c) throws IOException {
    BooleanQuery booleanQuery = new BooleanQuery();
    BooleanQuery subQuery = new BooleanQuery();
    for (String textFieldName : textFieldNames) {
     subQuery.add(new BooleanClause(new TermQuery(new Term(textFieldName, word)), BooleanClause.Occur.SHOULD));
    }
    booleanQuery.add(new BooleanClause(subQuery, BooleanClause.Occur.MUST));
    booleanQuery.add(new BooleanClause(new TermQuery(new Term(classFieldName, c)), BooleanClause.Occur.MUST));
    if (query != null) {
      booleanQuery.add(query, BooleanClause.Occur.MUST);
    }
    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
    indexSearcher.search(booleanQuery, totalHitCountCollector);
    return totalHitCountCollector.getTotalHits();
  }

  private double calculateLogPrior(BytesRef currentClass) throws IOException {
    return Math.log((double) docCount(currentClass)) - Math.log(docsWithClassSize);
  }

  private int docCount(BytesRef countedClass) throws IOException {
    return atomicReader.docFreq(new Term(classFieldName, countedClass));
  }
}