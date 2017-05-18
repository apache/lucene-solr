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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.similarities.Similarity.SimWeight;

/** 
 * Function that returns the decoded norm for every document.
 * <p>
 * Note that the configured Similarity for the field must be
 * a subclass of {@link TFIDFSimilarity} and the contribution of
 * the TF needs to be 1 when the freq is 1 and the contribution
 * of the IDF needs to be 1 when docFreq == docCount == 1.
 * @lucene.internal */
public class NormValueSource extends ValueSource {
  protected final String field;
  public NormValueSource(String field) {
    this.field = field;
  }

  public String name() {
    return "norm";
  }

  @Override
  public String description() {
    return name() + '(' + field + ')';
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    context.put("searcher",searcher);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    IndexSearcher searcher = (IndexSearcher)context.get("searcher");
    final TFIDFSimilarity similarity = IDFValueSource.asTFIDF(searcher.getSimilarity(true), field);
    if (similarity == null) {
      throw new UnsupportedOperationException("requires a TFIDFSimilarity (such as ClassicSimilarity)");
    }
    // Only works if the contribution of the tf is 1 when the freq is 1 and contribution of the idf
    // is 1 when docCount == docFreq == 1
    final SimWeight simWeight = similarity.computeWeight(1f,
        new CollectionStatistics(field, 1, 1, 1, 1),
        new TermStatistics(new BytesRef("bogus"), 1, 1));
    final SimScorer simScorer = similarity.simScorer(simWeight, readerContext);
    
    return new FloatDocValues(this) {
      int lastDocID = -1;
      @Override
      public float floatVal(int docID) throws IOException {
        if (docID < lastDocID) {
          throw new AssertionError("docs out of order: lastDocID=" + lastDocID + " docID=" + docID);
        }
        lastDocID = docID;
        return simScorer.score(docID, 1f);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) {
      return false;
    }
    return this.field.equals(((NormValueSource)o).field);
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode() + field.hashCode();
  }
}


