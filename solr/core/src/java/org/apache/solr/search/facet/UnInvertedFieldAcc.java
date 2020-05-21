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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.function.IntFunction;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;

/**
 * Base accumulator for {@link UnInvertedField}
 */
public abstract class UnInvertedFieldAcc extends SlotAcc implements UnInvertedField.Callback {

  UnInvertedField uif;
  UnInvertedField.DocToTerm docToTerm;
  SchemaField sf;

  public UnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
    super(fcontext);
    this.sf = sf;
    uif = UnInvertedField.getUnInvertedField(sf.getName(), fcontext.qcontext.searcher());
    docToTerm = uif.new DocToTerm();
    fcontext.qcontext.addCloseHook(this);
  }

  @Override
  public void close() throws IOException {
    if (docToTerm != null) {
      docToTerm.close();
      docToTerm = null;
    }
  }


  abstract static class DoubleUnInvertedFieldAcc extends UnInvertedFieldAcc {
    double[] result;
    int currentSlot;
    double initialValue;

  public DoubleUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots, double initialValue) throws IOException {
      super(fcontext, sf, numSlots);
      result = new double[numSlots];
      if (initialValue != 0) {
        this.initialValue = initialValue;
        Arrays.fill(result, initialValue);
      }
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      this.currentSlot = slot;
      docToTerm.getBigTerms(doc + currentDocBase, this);
      docToTerm.getSmallTerms(doc + currentDocBase, this);
    }

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(result[slotA], result[slotB]);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return result[slotNum];
    }

    @Override
    public void reset() throws IOException {
      Arrays.fill(result, initialValue);
    }

    @Override
    public void resize(Resizer resizer) {
    this.result = resizer.resize(result, initialValue);
    }
  }

  /**
   * Base accumulator to compute standard deviation and variance for uninvertible fields
   */
  abstract static class SDVUnInvertedFieldAcc extends DoubleUnInvertedFieldAcc {
    int[] counts;
    double[] sum;

  public SDVUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots, 0);
      this.counts = new int[numSlots];
      this.sum = new double[numSlots];
    }

    @Override
    public void call(int termNum) {
      try {
        BytesRef term = docToTerm.lookupOrd(termNum);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date ? ((Date) obj).getTime() : ((Number) obj).doubleValue();
        result[currentSlot] += val * val;
        sum[currentSlot] += val;
        counts[currentSlot]++;
      } catch (IOException e) {
        // find a better way to do it
        throw new UncheckedIOException(e);
      }
    }

    protected abstract double computeVal(int slot);

    @Override
    public int compare(int slotA, int slotB) {
      return Double.compare(computeVal(slotA), computeVal(slotB));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object getValue(int slot) {
      if (fcontext.isShard()) {
        ArrayList lst = new ArrayList(3);
        lst.add(counts[slot]);
        lst.add(result[slot]);
        lst.add(sum[slot]);
        return lst;
      } else {
        return computeVal(slot);
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      Arrays.fill(counts, 0);
      Arrays.fill(sum, 0);
    }

    @Override
    public void resize(Resizer resizer) {
      super.resize(resizer);
    this.counts = resizer.resize(counts, 0);
    this.sum = resizer.resize(sum, 0);
    }
  }
}
