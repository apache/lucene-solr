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

package org.apache.solr.search.join;

import java.io.IOException;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.util.LongIterator;
import org.apache.solr.util.LongSet;

/** @lucene.internal */
public class GraphPointsCollector extends GraphEdgeCollector {
  final LongSet set = new LongSet(256);

  SortedNumericDocValues values = null;

  public GraphPointsCollector(SchemaField collectField, DocSet skipSet, DocSet leafNodes) {
    super(collectField, skipSet, leafNodes);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    super.doSetNextReader(context);
    values = DocValues.getSortedNumeric(context.reader(), collectField.getName());
  }

  @Override
  void addEdgeIdsToResult(int doc) throws IOException {
    // set the doc to pull the edges ids for.
    int valuesDoc = values.docID();
    if (valuesDoc < doc) {
      valuesDoc = values.advance(doc);
    }
    if (valuesDoc == doc) {
      int count = values.docValueCount();
      for (int i = 0; i < count; i++) {
        // duplicates may be produced for a single doc, but won't matter here.
        long v = values.nextValue();
        set.add(v);
      }
    }
  }

  @Override
  public Query getResultQuery(SchemaField matchField, boolean useAutomaton) {
    if (set.cardinality() == 0) return null;

    Query q = null;

    // How we interpret the longs collected depends on the field we collect from (single valued can be diff from multi valued)
    // The basic type of the from & to field must match though (int/long/float/double)
    NumberType ntype = collectField.getType().getNumberType();
    boolean multiValued = collectField.multiValued();

    if (ntype == NumberType.LONG || ntype == NumberType.DATE) {
      long[] vals = new long[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        long v = bits;
        vals[i++] = v;
      }
      q = LongPoint.newSetQuery(matchField.getName(), vals);
    } else if (ntype == NumberType.INTEGER) {
      int[] vals = new int[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        int v = (int)bits;
        vals[i++] = v;
      }
      q = IntPoint.newSetQuery(matchField.getName(), vals);
    } else if (ntype == NumberType.DOUBLE) {
      double[] vals = new double[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        double v = multiValued ? NumericUtils.sortableLongToDouble(bits) : Double.longBitsToDouble(bits);
        vals[i++] = v;
      }
      q = DoublePoint.newSetQuery(matchField.getName(), vals);
    } else if (ntype == NumberType.FLOAT) {
      float[] vals = new float[set.cardinality()];
      int i = 0;
      for (LongIterator iter = set.iterator(); iter.hasNext(); ) {
        long bits = iter.next();
        float v = multiValued ? NumericUtils.sortableIntToFloat((int) bits) : Float.intBitsToFloat((int) bits);
        vals[i++] = v;
      }
      q = FloatPoint.newSetQuery(matchField.getName(), vals);
    }

    return q;
  }


}
