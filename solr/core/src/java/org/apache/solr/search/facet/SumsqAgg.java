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
package org.apache.solr.search.facet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FieldNameValueSource;

public class SumsqAgg extends SimpleAggValueSource {
  public SumsqAgg(ValueSource vs) {
    super("sumsq", vs);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();

    if (vs instanceof FieldNameValueSource) {
      String field = ((FieldNameValueSource)vs).getFieldName();
      SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(field);
      if (sf.getType().getNumberType() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            name() + " aggregation not supported for " + sf.getType().getTypeName());
      }
      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        if (sf.hasDocValues()) {
          if (sf.getType().isPointField()) {
            return new SumSqSortedNumericAcc(fcontext, sf, numSlots);
          }
          return new SumSqSortedSetAcc(fcontext, sf, numSlots);
        }
        if (sf.getType().isPointField()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              name() + " aggregation not supported for PointField w/o docValues");
        }
        return new SumSqUnInvertedFieldAcc(fcontext, sf, numSlots);
      }
      vs = sf.getType().getValueSource(sf, null);
    }
    return new SlotAcc.SumsqSlotAcc(vs, fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new SumAgg.Merger();
  }

  class SumSqSortedNumericAcc extends DocValuesAcc.DoubleSortedNumericDVAcc {

    public SumSqSortedNumericAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      for (int i = 0, count = values.docValueCount(); i < count; i++) {
        double val = getDouble(values.nextValue());
        result[slot]+= val * val;
      }
    }
  }

  class SumSqSortedSetAcc extends DocValuesAcc.DoubleSortedSetDVAcc {

    public SumSqSortedSetAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      long ord;
      while ((ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        BytesRef term = values.lookupOrd(ord);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date ? ((Date)obj).getTime(): ((Number)obj).doubleValue();
        result[slot] += val * val;
      }
    }
  }

  class SumSqUnInvertedFieldAcc extends UnInvertedFieldAcc.DoubleUnInvertedFieldAcc {

    public SumSqUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
    }

    @Override
    public void call(int termNum) {
      try {
        BytesRef term = docToTerm.lookupOrd(termNum);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date? ((Date)obj).getTime(): ((Number)obj).doubleValue();
        result[currentSlot] += val * val;
      } catch (IOException e) {
        // find a better way to do it
        throw new UncheckedIOException(e);
      }
    }
  }
}
