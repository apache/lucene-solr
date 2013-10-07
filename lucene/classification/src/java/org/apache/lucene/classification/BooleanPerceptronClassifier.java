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
import java.io.StringReader;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/**
 * A perceptron (see <code>http://en.wikipedia.org/wiki/Perceptron</code>) based
 * <code>Boolean</code> {@link org.apache.lucene.classification.Classifier}. The
 * weights are calculated using
 * {@link org.apache.lucene.index.TermsEnum#totalTermFreq} both on a per field
 * and a per document basis and then a corresponding
 * {@link org.apache.lucene.util.fst.FST} is used for class assignment.
 * 
 * @lucene.experimental
 */
public class BooleanPerceptronClassifier implements Classifier<Boolean> {

  private Double threshold;
  private final Integer batchSize;
  private Terms textTerms;
  private Analyzer analyzer;
  private String textFieldName;
  private FST<Long> fst;

  /**
   * Create a {@link BooleanPerceptronClassifier}
   * 
   * @param threshold
   *          the binary threshold for perceptron output evaluation
   */
  public BooleanPerceptronClassifier(Double threshold, Integer batchSize) {
    this.threshold = threshold;
    this.batchSize = batchSize;
  }

  /**
   * Default constructor, no batch updates of FST, perceptron threshold is
   * calculated via underlying index metrics during
   * {@link #train(org.apache.lucene.index.AtomicReader, String, String, org.apache.lucene.analysis.Analyzer)
   * training}
   */
  public BooleanPerceptronClassifier() {
    batchSize = 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassificationResult<Boolean> assignClass(String text)
      throws IOException {
    if (textTerms == null) {
      throw new IOException("You must first call Classifier#train");
    }
    Long output = 0l;
    try (TokenStream tokenStream = analyzer.tokenStream(textFieldName, text)) {
      CharTermAttribute charTermAttribute = tokenStream
        .addAttribute(CharTermAttribute.class);
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        String s = charTermAttribute.toString();
        Long d = Util.get(fst, new BytesRef(s));
        if (d != null) {
          output += d;
        }
      }
      tokenStream.end();
    }

    return new ClassificationResult<>(output >= threshold, output.doubleValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void train(AtomicReader atomicReader, String textFieldName,
      String classFieldName, Analyzer analyzer) throws IOException {
    this.textTerms = MultiFields.getTerms(atomicReader, textFieldName);

    if (textTerms == null) {
      throw new IOException(new StringBuilder(
          "term vectors need to be available for field ").append(textFieldName)
          .toString());
    }

    this.analyzer = analyzer;
    this.textFieldName = textFieldName;

    if (threshold == null || threshold == 0d) {
      // automatic assign a threshold
      long sumDocFreq = atomicReader.getSumDocFreq(textFieldName);
      if (sumDocFreq != -1) {
        this.threshold = (double) sumDocFreq / 2d;
      } else {
        throw new IOException(
            "threshold cannot be assigned since term vectors for field "
                + textFieldName + " do not exist");
      }
    }

    // TODO : remove this map as soon as we have a writable FST
    SortedMap<String,Double> weights = new TreeMap<>();

    TermsEnum reuse = textTerms.iterator(null);
    BytesRef textTerm;
    while ((textTerm = reuse.next()) != null) {
      weights.put(textTerm.utf8ToString(), (double) reuse.totalTermFreq());
    }
    updateFST(weights);

    IndexSearcher indexSearcher = new IndexSearcher(atomicReader);

    int batchCount = 0;

    // do a *:* search and use stored field values
    for (ScoreDoc scoreDoc : indexSearcher.search(new MatchAllDocsQuery(),
        Integer.MAX_VALUE).scoreDocs) {
      StoredDocument doc = indexSearcher.doc(scoreDoc.doc);

      // assign class to the doc
      ClassificationResult<Boolean> classificationResult = assignClass(doc
          .getField(textFieldName).stringValue());
      Boolean assignedClass = classificationResult.getAssignedClass();
      
      // get the expected result
      StorableField field = doc.getField(classFieldName);
      
      Boolean correctClass = Boolean.valueOf(field.stringValue());
      long modifier = correctClass.compareTo(assignedClass);
      if (modifier != 0) {
        reuse = updateWeights(atomicReader, reuse, scoreDoc.doc, assignedClass,
            weights, modifier, batchCount % batchSize == 0);
      }
      batchCount++;
    }
    weights.clear(); // free memory while waiting for GC
  }

  private TermsEnum updateWeights(AtomicReader atomicReader, TermsEnum reuse,
      int docId, Boolean assignedClass, SortedMap<String,Double> weights,
      double modifier, boolean updateFST) throws IOException {
    TermsEnum cte = textTerms.iterator(reuse);

    // get the doc term vectors
    Terms terms = atomicReader.getTermVector(docId, textFieldName);

    if (terms == null) {
      throw new IOException("term vectors must be stored for field "
          + textFieldName);
    }

    TermsEnum termsEnum = terms.iterator(null);

    BytesRef term;

    while ((term = termsEnum.next()) != null) {
      cte.seekExact(term);
      if (assignedClass != null) {
        long termFreqLocal = termsEnum.totalTermFreq();
        // update weights
        Long previousValue = Util.get(fst, term);
        String termString = term.utf8ToString();
        weights.put(termString, previousValue + modifier * termFreqLocal);
      }
    }
    if (updateFST) {
      updateFST(weights);
    }
    reuse = cte;
    return reuse;
  }

  private void updateFST(SortedMap<String,Double> weights) throws IOException {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> fstBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    BytesRef scratchBytes = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    for (Map.Entry<String,Double> entry : weights.entrySet()) {
      scratchBytes.copyChars(entry.getKey());
      fstBuilder.add(Util.toIntsRef(scratchBytes, scratchInts), entry
          .getValue().longValue());
    }
    fst = fstBuilder.finish();
  }

}