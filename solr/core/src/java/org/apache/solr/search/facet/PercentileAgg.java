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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import com.tdunning.math.stats.AVLTreeDigest;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

public class PercentileAgg extends SimpleAggValueSource {
  List<Double> percentiles;

  public PercentileAgg(ValueSource vs, List<Double> percentiles) {
    super("percentile", vs);
    this.percentiles = percentiles;
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    return new Acc(getArg(), fcontext, numSlots);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new Merger();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PercentileAgg)) return false;
    PercentileAgg other = (PercentileAgg)o;
    return this.arg.equals(other.arg) && this.percentiles.equals(other.percentiles);
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 31 + percentiles.hashCode();
  }

  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      List<Double> percentiles = new ArrayList<>();
      ValueSource vs = fp.parseValueSource();
      while (fp.hasMoreArguments()) {
        double val = fp.parseDouble();
        if (val<0 || val>100) {
          throw new SyntaxError("requested percentile must be between 0 and 100.  got " + val);
        }
        percentiles.add(val);
      }

      if (percentiles.isEmpty()) {
        throw new SyntaxError("expected percentile(valsource,percent1[,percent2]*)  EXAMPLE:percentile(myfield,50)");
      }

      return new PercentileAgg(vs, percentiles);
    }
  }


  protected Object getValueFromDigest(AVLTreeDigest digest) {
    if (digest == null) {
      return null;
    }

    if (percentiles.size() == 1) {
      return digest.quantile( percentiles.get(0) * 0.01 );
    }

    List<Double> lst = new ArrayList(percentiles.size());
    for (Double percentile : percentiles) {
      double val = digest.quantile( percentile * 0.01 );
      lst.add( val );
    }
    return lst;
  }



  class Acc extends FuncSlotAcc {
    protected AVLTreeDigest[] digests;
    protected ByteBuffer buf;
    protected double[] sortvals;

    public Acc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots);
      digests = new AVLTreeDigest[numSlots];
    }

    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      if (!values.exists(doc)) return;
      double val = values.doubleVal(doc);

      AVLTreeDigest digest = digests[slotNum];
      if (digest == null) {
        digests[slotNum] = digest = new AVLTreeDigest(100);   // TODO: make compression configurable
      }

      digest.add(val);
    }

    @Override
    public int compare(int slotA, int slotB) {
      if (sortvals == null) {
        fillSortVals();
      }
      return Double.compare(sortvals[slotA], sortvals[slotB]);
    }

    private void fillSortVals() {
      sortvals = new double[ digests.length ];
      double sortp = percentiles.get(0) * 0.01;
      for (int i=0; i<digests.length; i++) {
        AVLTreeDigest digest = digests[i];
        if (digest == null) {
          sortvals[i] = Double.NEGATIVE_INFINITY;
        } else {
          sortvals[i] = digest.quantile(sortp);
        }
      }
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      if (fcontext.isShard()) {
        return getShardValue(slotNum);
      }
      if (sortvals != null && percentiles.size()==1) {
        // we've already calculated everything we need
        return digests[slotNum] != null ? sortvals[slotNum] : null;
      }
      return getValueFromDigest( digests[slotNum] );
    }


    public Object getShardValue(int slot) throws IOException {
      AVLTreeDigest digest = digests[slot];
      if (digest == null) return null;  // no values for this slot

      digest.compress();
      int sz = digest.byteSize();
      if (buf == null || buf.capacity() < sz) {
        buf = ByteBuffer.allocate(sz+(sz>>1));  // oversize by 50%
      } else {
        buf.clear();
      }
      digest.asSmallBytes(buf);
      byte[] arr = Arrays.copyOf(buf.array(), buf.position());
      return arr;
    }


    @Override
    public void reset() {
      digests = new AVLTreeDigest[digests.length];
      sortvals = null;
    }

    @Override
    public void resize(Resizer resizer) {
      digests = resizer.resize(digests, null);
    }
  }


  class Merger extends FacetSortableMerger {
    protected AVLTreeDigest digest;
    protected Double sortVal;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      byte[] arr = (byte[])facetResult;
      if (arr == null) return; // an explicit null can mean no values in the field
      AVLTreeDigest subDigest = AVLTreeDigest.fromBytes(ByteBuffer.wrap(arr));
      if (digest == null) {
        digest = subDigest;
      } else {
        digest.add(subDigest);
      }
    }

    @Override
    public Object getMergedResult() {
      if (percentiles.size() == 1 && digest != null) return getSortVal();
      return getValueFromDigest(digest);
    }

    @Override
    public int compareTo(FacetSortableMerger other, FacetRequest.SortDirection direction) {
      return Double.compare(getSortVal(), ((Merger) other).getSortVal());
    }

    private Double getSortVal() {
      if (sortVal == null) {
        sortVal = digest==null ? Double.NEGATIVE_INFINITY : digest.quantile( percentiles.get(0) * 0.01 );
      }
      return sortVal;
    }
  }
}

