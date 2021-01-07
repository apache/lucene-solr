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
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueFloat;

/**
 * <code>QueryValueSource</code> returns the relevance score of the query
 */
public class QueryValueSource extends ValueSource {
  final Query q;
  final float defVal;

  public QueryValueSource(Query q, float defVal) {
    super();
    if (q == null) {
      throw new IllegalArgumentException("query cannot be null");
    }
    this.q = q;
    this.defVal = defVal;
  }

  public Query getQuery() { return q; }
  public float getDefaultValue() { return defVal; }

  @Override
  public String description() {
    return "query(" + q + ",def=" + defVal + ")";
  }

  @Override
  public FunctionValues getValues(Map fcontext, LeafReaderContext readerContext) throws IOException {
    return new QueryDocValues(this, readerContext, fcontext);
  }

  @Override
  public int hashCode() {
    return q.hashCode() * 29;
  }

  @Override
  public boolean equals(Object o) {
    if (QueryValueSource.class != o.getClass()) return false;
    QueryValueSource other = (QueryValueSource)o;
    return this.q.equals(other.q) && this.defVal==other.defVal;
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    Query rewritten = searcher.rewrite(q);
    Weight w = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
    context.put(this, w);
  }
}


class QueryDocValues extends FloatDocValues {
  final LeafReaderContext readerContext;
  final Weight weight;
  final float defVal;
  final Map fcontext;
  final Query q;

  Scorer scorer;
  DocIdSetIterator disi;
  TwoPhaseIterator tpi;
  Boolean thisDocMatches;

  // the last document requested
  int lastDocRequested=-1;


  public QueryDocValues(QueryValueSource vs, LeafReaderContext readerContext, Map fcontext) throws IOException {
    super(vs);

    this.readerContext = readerContext;
    this.defVal = vs.defVal;
    this.q = vs.q;
    this.fcontext = fcontext;

    Weight w = fcontext==null ? null : (Weight)fcontext.get(vs);
    if (w == null) {
      IndexSearcher weightSearcher;
      if(fcontext == null) {
        weightSearcher = new IndexSearcher(ReaderUtil.getTopLevelContext(readerContext));
      } else {
        weightSearcher = (IndexSearcher)fcontext.get("searcher");
        if (weightSearcher == null) {
          weightSearcher = new IndexSearcher(ReaderUtil.getTopLevelContext(readerContext));
        }
      }
      vs.createWeight(fcontext, weightSearcher);
      w = (Weight)fcontext.get(vs);
    }
    weight = w;
  }

  @Override
  public float floatVal(int doc) {
    try {
      return exists(doc) ? scorer.score() : defVal;
    } catch (IOException e) {
      throw new RuntimeException("caught exception in QueryDocVals("+q+") doc="+doc, e);
    }
  }

  @Override
  public boolean exists(int doc) {
    if (doc < lastDocRequested) {
      throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocRequested + " vs docID=" + doc);
    }
    lastDocRequested = doc;

    try {
      if (disi == null) {
        scorer = weight.scorer(readerContext);
        if (scorer == null) {
          disi = DocIdSetIterator.empty();
        } else {
          tpi = scorer.twoPhaseIterator();
          disi = tpi == null ? scorer.iterator() : tpi.approximation();
        }
        thisDocMatches = null;
      }

      if (disi.docID() < doc) {
        disi.advance(doc);
        thisDocMatches = null;
      }
      if (disi.docID() == doc) {
        if (thisDocMatches == null) {
          thisDocMatches = tpi == null || tpi.matches();
        }
        return thisDocMatches;
      } else return false;
    } catch (IOException e) {
      throw new RuntimeException("caught exception in QueryDocVals("+q+") doc="+doc, e);
    }
  }

  @Override
  public Object objectVal(int doc) {
    return floatVal(doc);
  }

  @Override
  public ValueFiller getValueFiller() {
    //
    // TODO: if we want to support more than one value-filler or a value-filler in conjunction with
    // the FunctionValues, then members like "scorer" should be per ValueFiller instance.
    // Or we can say that the user should just instantiate multiple FunctionValues.
    //
    return new ValueFiller() {
      private final MutableValueFloat mval = new MutableValueFloat();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        try {
          if (exists(doc)) {
            mval.value = scorer.score();
            mval.exists = true;
          } else {
            mval.value = defVal;
            mval.exists = false;
          }
        } catch (IOException e) {
          throw new RuntimeException("caught exception in QueryDocVals("+q+") doc="+doc, e);
        }
      }
    };
  }

  @Override
  public String toString(int doc) {
    return "query(" + q + ",def=" + defVal + ")=" + floatVal(doc);
  }
}