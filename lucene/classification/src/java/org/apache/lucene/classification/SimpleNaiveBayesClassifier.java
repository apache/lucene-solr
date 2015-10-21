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
import org.apache.lucene.index.LeafReader;
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

/**
 * A simplistic Lucene based NaiveBayes classifier, see <code>http://en.wikipedia.org/wiki/Naive_Bayes_classifier</code>
 *
 * @lucene.experimental
 */
public class SimpleNaiveBayesClassifier implements Classifier<BytesRef> {

  /**
   * {@link org.apache.lucene.index.LeafReader} used to access the {@link org.apache.lucene.classification.Classifier}'s
   * index
   */
  protected LeafReader leafReader;

  /**
   * names of the fields to be used as input text
   */
  protected String[] textFieldNames;

  /**
   * name of the field to be used as a class / category output
   */
  protected String classFieldName;

  /**
   * {@link org.apache.lucene.analysis.Analyzer} to be used for tokenizing unseen input text
   */
  protected Analyzer analyzer;

  /**
   * {@link org.apache.lucene.search.IndexSearcher} to run searches on the index for retrieving frequencies
   */
  protected IndexSearcher indexSearcher;

  /**
   * {@link org.apache.lucene.search.Query} used to eventually filter the document set to be used to classify
   */
  protected Query query;

  /**
   * Creates a new NaiveBayes classifier.
   * Note that you must call {@link #train(org.apache.lucene.index.LeafReader, String, String, Analyzer) train()} before you can
   * classify any documents.
   */
  public SimpleNaiveBayesClassifier() {
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
  public void train(LeafReader leafReader, String textFieldName, String classFieldName, Analyzer analyzer, Query query)
      throws IOException {
    train(leafReader, new String[]{textFieldName}, classFieldName, analyzer, query);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(LeafReader leafReader, String[] textFieldNames, String classFieldName, Analyzer analyzer, Query query)
      throws IOException {
    this.leafReader = leafReader;
    this.indexSearcher = new IndexSearcher(this.leafReader);
    this.textFieldNames = textFieldNames;
    this.classFieldName = classFieldName;
    this.analyzer = analyzer;
    this.query = query;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassificationResult<BytesRef> assignClass(String inputDocument) throws IOException {
    List<ClassificationResult<BytesRef>> doclist = assignClassNormalizedList(inputDocument);
    ClassificationResult<BytesRef> retval = null;
    double maxscore = -Double.MAX_VALUE;
    for (ClassificationResult<BytesRef> element : doclist) {
      if (element.getScore() > maxscore) {
        retval = element;
        maxscore = element.getScore();
      }
    }
    return retval;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text) throws IOException {
    List<ClassificationResult<BytesRef>> doclist = assignClassNormalizedList(text);
    Collections.sort(doclist);
    return doclist;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text, int max) throws IOException {
    List<ClassificationResult<BytesRef>> doclist = assignClassNormalizedList(text);
    Collections.sort(doclist);
    return doclist.subList(0, max);
  }

  private List<ClassificationResult<BytesRef>> assignClassNormalizedList(String inputDocument) throws IOException {
    if (leafReader == null) {
      throw new IOException("You must first call Classifier#train");
    }
    List<ClassificationResult<BytesRef>> dataList = new ArrayList<>();

    Terms terms = MultiFields.getTerms(leafReader, classFieldName);
    TermsEnum termsEnum = terms.iterator();
    BytesRef next;
    String[] tokenizedDoc = tokenizeDoc(inputDocument);
    int docsWithClassSize = countDocsWithClass();
    while ((next = termsEnum.next()) != null) {
      if (next.length > 0) {
        // We are passing the term to IndexSearcher so we need to make sure it will not change over time
        Term term = new Term(this.classFieldName, next);
        double clVal = calculateLogPrior(term, docsWithClassSize) + calculateLogLikelihood(tokenizedDoc, term, docsWithClassSize);
        dataList.add(new ClassificationResult<>(term.bytes(), clVal));
      }
    }

    // normalization; the values transforms to a 0-1 range
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

  /**
   * count the number of documents in the index having at least a value for the 'class' field
   *
   * @return the no. of documents having a value for the 'class' field
   * @throws IOException if accessing to term vectors or search fails
   */
  protected int countDocsWithClass() throws IOException {
    int docCount = MultiFields.getTerms(this.leafReader, this.classFieldName).getDocCount();
    if (docCount == -1) { // in case codec doesn't support getDocCount
      TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.add(new BooleanClause(new WildcardQuery(new Term(classFieldName, String.valueOf(WildcardQuery.WILDCARD_STRING))), BooleanClause.Occur.MUST));
      if (query != null) {
        q.add(query, BooleanClause.Occur.MUST);
      }
      indexSearcher.search(q.build(),
          totalHitCountCollector);
      docCount = totalHitCountCollector.getTotalHits();
    }
    return docCount;
  }

  /**
   * tokenize a <code>String</code> on this classifier's text fields and analyzer
   *
   * @param doc the <code>String</code> representing an input text (to be classified)
   * @return a <code>String</code> array of the resulting tokens
   * @throws IOException if tokenization fails
   */
  protected String[] tokenizeDoc(String doc) throws IOException {
    Collection<String> result = new LinkedList<>();
    for (String textFieldName : textFieldNames) {
      try (TokenStream tokenStream = analyzer.tokenStream(textFieldName, doc)) {
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
      Terms terms = MultiFields.getTerms(leafReader, textFieldName);
      long numPostings = terms.getSumDocFreq(); // number of term/doc pairs
      avgNumberOfUniqueTerms += numPostings / (double) terms.getDocCount(); // avg # of unique terms per doc
    }
    int docsWithC = leafReader.docFreq(term);
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
    return leafReader.docFreq(term);
  }
}
