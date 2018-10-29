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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
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

/**
 * A simplistic Lucene based NaiveBayes classifier, see <code>http://en.wikipedia.org/wiki/Naive_Bayes_classifier</code>
 *
 * @lucene.experimental
 */
public class SimpleNaiveBayesClassifier implements Classifier<BytesRef> {

  /**
   * {@link org.apache.lucene.index.IndexReader} used to access the {@link org.apache.lucene.classification.Classifier}'s
   * index
   */
  protected final IndexReader indexReader;

  /**
   * names of the fields to be used as input text
   */
  protected final String[] textFieldNames;

  /**
   * name of the field to be used as a class / category output
   */
  protected final String classFieldName;

  /**
   * {@link org.apache.lucene.analysis.Analyzer} to be used for tokenizing unseen input text
   */
  protected final Analyzer analyzer;

  /**
   * {@link org.apache.lucene.search.IndexSearcher} to run searches on the index for retrieving frequencies
   */
  protected final IndexSearcher indexSearcher;

  /**
   * {@link org.apache.lucene.search.Query} used to eventually filter the document set to be used to classify
   */
  protected final Query query;

  /**
   * Creates a new NaiveBayes classifier.
   *
   * @param indexReader     the reader on the index to be used for classification
   * @param analyzer       an {@link Analyzer} used to analyze unseen text
   * @param query          a {@link Query} to eventually filter the docs used for training the classifier, or {@code null}
   *                       if all the indexed docs should be used
   * @param classFieldName the name of the field used as the output for the classifier NOTE: must not be havely analyzed
   *                       as the returned class will be a token indexed for this field
   * @param textFieldNames the name of the fields used as the inputs for the classifier, NO boosting supported per field
   */
  public SimpleNaiveBayesClassifier(IndexReader indexReader, Analyzer analyzer, Query query, String classFieldName, String... textFieldNames) {
    this.indexReader = indexReader;
    this.indexSearcher = new IndexSearcher(this.indexReader);
    this.textFieldNames = textFieldNames;
    this.classFieldName = classFieldName;
    this.analyzer = analyzer;
    this.query = query;
  }

  @Override
  public ClassificationResult<BytesRef> assignClass(String inputDocument) throws IOException {
    List<ClassificationResult<BytesRef>> assignedClasses = assignClassNormalizedList(inputDocument);
    ClassificationResult<BytesRef> assignedClass = null;
    double maxscore = -Double.MAX_VALUE;
    for (ClassificationResult<BytesRef> c : assignedClasses) {
      if (c.getScore() > maxscore) {
        assignedClass = c;
        maxscore = c.getScore();
      }
    }
    return assignedClass;
  }

  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text) throws IOException {
    List<ClassificationResult<BytesRef>> assignedClasses = assignClassNormalizedList(text);
    Collections.sort(assignedClasses);
    return assignedClasses;
  }

  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text, int max) throws IOException {
    List<ClassificationResult<BytesRef>> assignedClasses = assignClassNormalizedList(text);
    Collections.sort(assignedClasses);
    return assignedClasses.subList(0, max);
  }

  /**
   * Calculate probabilities for all classes for a given input text
   * @param inputDocument the input text as a {@code String}
   * @return a {@code List} of {@code ClassificationResult}, one for each existing class
   * @throws IOException if assigning probabilities fails
   */
  protected List<ClassificationResult<BytesRef>> assignClassNormalizedList(String inputDocument) throws IOException {
    List<ClassificationResult<BytesRef>> assignedClasses = new ArrayList<>();

    Terms classes = MultiTerms.getTerms(indexReader, classFieldName);
    if (classes != null) {
      TermsEnum classesEnum = classes.iterator();
      BytesRef next;
      String[] tokenizedText = tokenize(inputDocument);
      int docsWithClassSize = countDocsWithClass();
      while ((next = classesEnum.next()) != null) {
        if (next.length > 0) {
          Term term = new Term(this.classFieldName, next);
          double clVal = calculateLogPrior(term, docsWithClassSize) + calculateLogLikelihood(tokenizedText, term, docsWithClassSize);
          assignedClasses.add(new ClassificationResult<>(term.bytes(), clVal));
        }
      }
    }
    // normalization; the values transforms to a 0-1 range
    return normClassificationResults(assignedClasses);
  }

  /**
   * count the number of documents in the index having at least a value for the 'class' field
   *
   * @return the no. of documents having a value for the 'class' field
   * @throws IOException if accessing to term vectors or search fails
   */
  protected int countDocsWithClass() throws IOException {
    Terms terms = MultiTerms.getTerms(this.indexReader, this.classFieldName);
    int docCount;
    if (terms == null || terms.getDocCount() == -1) { // in case codec doesn't support getDocCount
      TotalHitCountCollector classQueryCountCollector = new TotalHitCountCollector();
      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.add(new BooleanClause(new WildcardQuery(new Term(classFieldName, String.valueOf(WildcardQuery.WILDCARD_STRING))), BooleanClause.Occur.MUST));
      if (query != null) {
        q.add(query, BooleanClause.Occur.MUST);
      }
      indexSearcher.search(q.build(),
          classQueryCountCollector);
      docCount = classQueryCountCollector.getTotalHits();
    } else {
      docCount = terms.getDocCount();
    }
    return docCount;
  }

  /**
   * tokenize a <code>String</code> on this classifier's text fields and analyzer
   *
   * @param text the <code>String</code> representing an input text (to be classified)
   * @return a <code>String</code> array of the resulting tokens
   * @throws IOException if tokenization fails
   */
  protected String[] tokenize(String text) throws IOException {
    Collection<String> result = new LinkedList<>();
    for (String textFieldName : textFieldNames) {
      try (TokenStream tokenStream = analyzer.tokenStream(textFieldName, text)) {
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
          result.add(charTermAttribute.toString());
        }
        tokenStream.end();
      }
    }
    return result.toArray(new String[result.size()]);
  }

  private double calculateLogLikelihood(String[] tokenizedText, Term term, int docsWithClass) throws IOException {
    // for each word
    double result = 0d;
    for (String word : tokenizedText) {
      // search with text:word AND class:c
      int hits = getWordFreqForClass(word, term);

      // num : count the no of times the word appears in documents of class c (+1)
      double num = hits + 1; // +1 is added because of add 1 smoothing

      // den : for the whole dictionary, count the no of times a word appears in documents of class c (+|V|)
      double den = getTextTermFreqForClass(term) + docsWithClass;

      // P(w|c) = num/den
      double wordProbability = num / den;
      result += Math.log(wordProbability);
    }

    // log(P(d|c)) = log(P(w1|c))+...+log(P(wn|c))
    return result;
  }

  /**
   * Returns the average number of unique terms times the number of docs belonging to the input class
   * @param term the term representing the class
   * @return the average number of unique terms
   * @throws IOException if a low level I/O problem happens
   */
  private double getTextTermFreqForClass(Term term) throws IOException {
    double avgNumberOfUniqueTerms = 0;
    for (String textFieldName : textFieldNames) {
      Terms terms = MultiTerms.getTerms(indexReader, textFieldName);
      long numPostings = terms.getSumDocFreq(); // number of term/doc pairs
      avgNumberOfUniqueTerms += numPostings / (double) terms.getDocCount(); // avg # of unique terms per doc
    }
    int docsWithC = indexReader.docFreq(term);
    return avgNumberOfUniqueTerms * docsWithC; // avg # of unique terms in text fields per doc * # docs with c
  }

  /**
   * Returns the number of documents of the input class ( from the whole index or from a subset)
   * that contains the word ( in a specific field or in all the fields if no one selected)
   * @param word the token produced by the analyzer
   * @param term the term representing the class
   * @return the number of documents of the input class
   * @throws IOException if a low level I/O problem happens
   */
  private int getWordFreqForClass(String word, Term term) throws IOException {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    BooleanQuery.Builder subQuery = new BooleanQuery.Builder();
    for (String textFieldName : textFieldNames) {
      subQuery.add(new BooleanClause(new TermQuery(new Term(textFieldName, word)), BooleanClause.Occur.SHOULD));
    }
    booleanQuery.add(new BooleanClause(subQuery.build(), BooleanClause.Occur.MUST));
    booleanQuery.add(new BooleanClause(new TermQuery(term), BooleanClause.Occur.MUST));
    if (query != null) {
      booleanQuery.add(query, BooleanClause.Occur.MUST);
    }
    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
    indexSearcher.search(booleanQuery.build(), totalHitCountCollector);
    return totalHitCountCollector.getTotalHits();
  }

  private double calculateLogPrior(Term term, int docsWithClassSize) throws IOException {
    return Math.log((double) docCount(term)) - Math.log(docsWithClassSize);
  }

  private int docCount(Term term) throws IOException {
    return indexReader.docFreq(term);
  }

  /**
   * Normalize the classification results based on the max score available
   * @param assignedClasses the list of assigned classes
   * @return the normalized results
   */
  protected ArrayList<ClassificationResult<BytesRef>> normClassificationResults(List<ClassificationResult<BytesRef>> assignedClasses) {
    // normalization; the values transforms to a 0-1 range
    ArrayList<ClassificationResult<BytesRef>> returnList = new ArrayList<>();
    if (!assignedClasses.isEmpty()) {
      Collections.sort(assignedClasses);
      // this is a negative number closest to 0 = a
      double smax = assignedClasses.get(0).getScore();

      double sumLog = 0;
      // log(sum(exp(x_n-a)))
      for (ClassificationResult<BytesRef> cr : assignedClasses) {
        // getScore-smax <=0 (both negative, smax is the smallest abs()
        sumLog += Math.exp(cr.getScore() - smax);
      }
      // loga=a+log(sum(exp(x_n-a))) = log(sum(exp(x_n)))
      double loga = smax;
      loga += Math.log(sumLog);

      // 1/sum*x = exp(log(x))*1/sum = exp(log(x)-log(sum))
      for (ClassificationResult<BytesRef> cr : assignedClasses) {
        double scoreDiff = cr.getScore() - loga;
        returnList.add(new ClassificationResult<>(cr.getAssignedClass(), Math.exp(scoreDiff)));
      }
    }
    return returnList;
  }
}
