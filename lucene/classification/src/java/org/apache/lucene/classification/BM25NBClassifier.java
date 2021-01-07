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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.util.BytesRef;

/**
 * A classifier approximating naive bayes classifier by using pure queries on BM25.
 *
 * @lucene.experimental
 */
public class BM25NBClassifier implements Classifier<BytesRef> {

  /** {@link IndexReader} used to access the {@link Classifier}'s index */
  private final IndexReader indexReader;

  /** names of the fields to be used as input text */
  private final String[] textFieldNames;

  /** name of the field to be used as a class / category output */
  private final String classFieldName;

  /** {@link Analyzer} to be used for tokenizing unseen input text */
  private final Analyzer analyzer;

  /** {@link IndexSearcher} to run searches on the index for retrieving frequencies */
  private final IndexSearcher indexSearcher;

  /** {@link Query} used to eventually filter the document set to be used to classify */
  private final Query query;

  /**
   * Creates a new NaiveBayes classifier.
   *
   * @param indexReader the reader on the index to be used for classification
   * @param analyzer an {@link Analyzer} used to analyze unseen text
   * @param query a {@link Query} to eventually filter the docs used for training the classifier, or
   *     {@code null} if all the indexed docs should be used
   * @param classFieldName the name of the field used as the output for the classifier NOTE: must
   *     not be heavely analyzed as the returned class will be a token indexed for this field
   * @param textFieldNames the name of the fields used as the inputs for the classifier, NO boosting
   *     supported per field
   */
  public BM25NBClassifier(
      IndexReader indexReader,
      Analyzer analyzer,
      Query query,
      String classFieldName,
      String... textFieldNames) {
    this.indexReader = indexReader;
    this.indexSearcher = new IndexSearcher(this.indexReader);
    this.indexSearcher.setSimilarity(new BM25Similarity());
    this.textFieldNames = textFieldNames;
    this.classFieldName = classFieldName;
    this.analyzer = analyzer;
    this.query = query;
  }

  @Override
  public ClassificationResult<BytesRef> assignClass(String inputDocument) throws IOException {
    return assignClassNormalizedList(inputDocument).get(0);
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
   *
   * @param inputDocument the input text as a {@code String}
   * @return a {@code List} of {@code ClassificationResult}, one for each existing class
   * @throws IOException if assigning probabilities fails
   */
  private List<ClassificationResult<BytesRef>> assignClassNormalizedList(String inputDocument)
      throws IOException {
    List<ClassificationResult<BytesRef>> assignedClasses = new ArrayList<>();

    Terms classes = MultiTerms.getTerms(indexReader, classFieldName);
    TermsEnum classesEnum = classes.iterator();
    BytesRef next;
    String[] tokenizedText = tokenize(inputDocument);
    while ((next = classesEnum.next()) != null) {
      if (next.length > 0) {
        Term term = new Term(this.classFieldName, next);
        assignedClasses.add(
            new ClassificationResult<>(
                term.bytes(),
                calculateLogPrior(term) + calculateLogLikelihood(tokenizedText, term)));
      }
    }

    return normClassificationResults(assignedClasses);
  }

  /**
   * Normalize the classification results based on the max score available
   *
   * @param assignedClasses the list of assigned classes
   * @return the normalized results
   */
  private ArrayList<ClassificationResult<BytesRef>> normClassificationResults(
      List<ClassificationResult<BytesRef>> assignedClasses) {
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

  /**
   * tokenize a <code>String</code> on this classifier's text fields and analyzer
   *
   * @param text the <code>String</code> representing an input text (to be classified)
   * @return a <code>String</code> array of the resulting tokens
   * @throws IOException if tokenization fails
   */
  private String[] tokenize(String text) throws IOException {
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
    return result.toArray(new String[0]);
  }

  private double calculateLogLikelihood(String[] tokens, Term term) throws IOException {
    double result = 0d;
    for (String word : tokens) {
      result += Math.log(getTermProbForClass(term, word));
    }
    return result;
  }

  private double getTermProbForClass(Term classTerm, String... words) throws IOException {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new BooleanClause(new TermQuery(classTerm), BooleanClause.Occur.MUST));
    for (String textFieldName : textFieldNames) {
      for (String word : words) {
        builder.add(
            new BooleanClause(
                new TermQuery(new Term(textFieldName, word)), BooleanClause.Occur.SHOULD));
      }
    }
    if (query != null) {
      builder.add(query, BooleanClause.Occur.MUST);
    }
    TopDocs search = indexSearcher.search(builder.build(), 1);
    return search.totalHits.value > 0 ? search.scoreDocs[0].score : 1;
  }

  private double calculateLogPrior(Term term) throws IOException {
    TermQuery termQuery = new TermQuery(term);
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(termQuery, BooleanClause.Occur.MUST);
    if (query != null) {
      bq.add(query, BooleanClause.Occur.MUST);
    }
    TopDocs topDocs = indexSearcher.search(bq.build(), 1);
    return topDocs.totalHits.value > 0 ? Math.log(topDocs.scoreDocs[0].score) : 0;
  }
}
