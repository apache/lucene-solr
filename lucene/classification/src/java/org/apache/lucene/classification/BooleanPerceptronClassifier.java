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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
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

  private final Double threshold;
  private final Terms textTerms;
  private final Analyzer analyzer;
  private final String textFieldName;
  private FST<Long> fst;

  public BooleanPerceptronClassifier(LeafReader leafReader, String textFieldName, String classFieldName, Analyzer analyzer,
                                     Query query, Integer batchSize, Double threshold) throws IOException {
    this.textTerms = MultiFields.getTerms(leafReader, textFieldName);

    if (textTerms == null) {
      throw new IOException("term vectors need to be available for field " + textFieldName);
    }

    this.analyzer = analyzer;
    this.textFieldName = textFieldName;

    if (threshold == null || threshold == 0d) {
      // automatic assign a threshold
      long sumDocFreq = leafReader.getSumDocFreq(textFieldName);
      if (sumDocFreq != -1) {
        this.threshold = (double) sumDocFreq / 2d;
      } else {
        throw new IOException(
                "threshold cannot be assigned since term vectors for field "
                        + textFieldName + " do not exist");
      }
    } else {
      this.threshold = threshold;
    }

    // TODO : remove this map as soon as we have a writable FST
    SortedMap<String, Double> weights = new ConcurrentSkipListMap<>();

    TermsEnum termsEnum = textTerms.iterator();
    BytesRef textTerm;
    while ((textTerm = termsEnum.next()) != null) {
      weights.put(textTerm.utf8ToString(), (double) termsEnum.totalTermFreq());
    }
    updateFST(weights);

    IndexSearcher indexSearcher = new IndexSearcher(leafReader);

    int batchCount = 0;

    BooleanQuery q = new BooleanQuery();
    q.add(new BooleanClause(new WildcardQuery(new Term(classFieldName, "*")), BooleanClause.Occur.MUST));
    if (query != null) {
      q.add(new BooleanClause(query, BooleanClause.Occur.MUST));
    }
    // run the search and use stored field values
    for (ScoreDoc scoreDoc : indexSearcher.search(q,
            Integer.MAX_VALUE).scoreDocs) {
      StoredDocument doc = indexSearcher.doc(scoreDoc.doc);

      StorableField textField = doc.getField(textFieldName);

      // get the expected result
      StorableField classField = doc.getField(classFieldName);

      if (textField != null && classField != null) {
        // assign class to the doc
        ClassificationResult<Boolean> classificationResult = assignClass(textField.stringValue());
        Boolean assignedClass = classificationResult.getAssignedClass();

        Boolean correctClass = Boolean.valueOf(classField.stringValue());
        long modifier = correctClass.compareTo(assignedClass);
        if (modifier != 0) {
          updateWeights(leafReader, scoreDoc.doc, assignedClass,
                  weights, modifier, batchCount % batchSize == 0);
        }
        batchCount++;
      }
    }
    weights.clear(); // free memory while waiting for GC
  }

  private void updateWeights(LeafReader leafReader,
                             int docId, Boolean assignedClass, SortedMap<String, Double> weights,
                             double modifier, boolean updateFST) throws IOException {
    TermsEnum cte = textTerms.iterator();

    // get the doc term vectors
    Terms terms = leafReader.getTermVector(docId, textFieldName);

    if (terms == null) {
      throw new IOException("term vectors must be stored for field "
              + textFieldName);
    }

    TermsEnum termsEnum = terms.iterator();

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
  }

  private void updateFST(SortedMap<String, Double> weights) throws IOException {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> fstBuilder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
    BytesRefBuilder scratchBytes = new BytesRefBuilder();
    IntsRefBuilder scratchInts = new IntsRefBuilder();
    for (Map.Entry<String, Double> entry : weights.entrySet()) {
      scratchBytes.copyChars(entry.getKey());
      fstBuilder.add(Util.toIntsRef(scratchBytes.get(), scratchInts), entry
              .getValue().longValue());
    }
    fst = fstBuilder.finish();
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

    double score = 1 - Math.exp(-1 * Math.abs(threshold - output.doubleValue()) / threshold);
    return new ClassificationResult<>(output >= threshold, score);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<Boolean>> getClasses(String text)
          throws IOException {
    throw new RuntimeException("not implemented");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<Boolean>> getClasses(String text, int max)
          throws IOException {
    throw new RuntimeException("not implemented");
  }

}
