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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.JavaBinDecoder;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.function.FieldNameValueSource;

public class PercentileAgg extends SimpleAggValueSource {
  public enum Digest {
    AVL_TREE,
    MERGING,
  }

  private final List<Double> percentiles;
  private final Supplier<TDigest> digestSupplier;
  private final Function<ByteBuffer, TDigest> digestParser;

  private PercentileAgg(ValueSource vs, List<Double> percentiles, Supplier<TDigest> digestSupplier, Function<ByteBuffer, TDigest> digestParser) {
    super("percentile", vs);
    this.percentiles = percentiles;
    this.digestSupplier = digestSupplier;
    this.digestParser = digestParser;
  }

  public static PercentileAgg create(ValueSource vs, List<Double> percentiles, Digest digest, double compression) {
    Supplier<TDigest> digestSupplier;
    Function<ByteBuffer, TDigest> digestParser;
    switch (digest) {
      case AVL_TREE:
        digestSupplier = () -> new AVLTreeDigest(compression);
        digestParser = AVLTreeDigest::fromBytes;
        break;
      case MERGING:
        digestSupplier = () -> new MergingDigest(compression);
        digestParser = MergingDigest::fromBytes;
        break;
      default:
        throw new IllegalArgumentException("unknown digest");
    }
    return new PercentileAgg(vs, percentiles, digestSupplier, digestParser);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    ValueSource vs = getArg();

    if (vs instanceof FieldNameValueSource) {
      String field = ((FieldNameValueSource) vs).getFieldName();
      SchemaField sf = fcontext.qcontext.searcher().getSchema().getField(field);
      if (sf.getType().getNumberType() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            name() + " aggregation not supported for " + sf.getType().getTypeName());
      }
      if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
        if (sf.hasDocValues()) {
          if (sf.getType().isPointField()) {
            return new PercentileSortedNumericAcc(fcontext, sf, numSlots);
          }
          return new PercentileSortedSetAcc(fcontext, sf, numSlots);
        }
        if (sf.getType().isPointField()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              name() + " aggregation not supported for PointField w/o docValues");
        }
        return new PercentileUnInvertedFieldAcc(fcontext, sf, numSlots);
      }
      vs = sf.getType().getValueSource(sf, null);
    }
    return new Acc(vs, fcontext, numSlots);
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

      return create(vs, percentiles, Digest.AVL_TREE, 100);
    }
  }

  protected Object getValueFromDigest(TDigest digest) {
    if (digest == null) {
      return null;
    }

    if (percentiles.size() == 1) {
      return digest.quantile( percentiles.get(0) * 0.01 );
    }

    List<Double> lst = new ArrayList<>(percentiles.size());
    for (Double percentile : percentiles) {
      double val = digest.quantile( percentile * 0.01 );
      lst.add( val );
    }
    return lst;
  }

  class Acc extends SlotAcc.FuncSlotAcc {
    protected TDigest[] digests;
    protected ByteBuffer buf;
    protected double[] sortvals;

    public Acc(ValueSource values, FacetContext fcontext, int numSlots) {
      super(values, fcontext, numSlots);
      digests = new TDigest[numSlots];
    }

    public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
      if (!values.exists(doc)) return;
      double val = values.doubleVal(doc);

      TDigest digest = digests[slotNum];
      if (digest == null) {
        digests[slotNum] = digest = digestSupplier.get();
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
      for (int i = 0; i < digests.length; i++) {
        TDigest digest = digests[i];
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
      TDigest digest = digests[slot];
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
      digests = new TDigest[digests.length];
      sortvals = null;
    }

    @Override
    public void resize(Resizer resizer) {
      digests = resizer.resize(digests, null);
    }
  }

  abstract class BasePercentileDVAcc extends DocValuesAcc {
    TDigest[] digests;
    protected ByteBuffer buf;
    double[] sortvals;

    public BasePercentileDVAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf);
      digests = new TDigest[numSlots];
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
      for (int i = 0; i < digests.length; i++) {
        TDigest digest = digests[i];
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
      TDigest digest = digests[slot];
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
      digests = new TDigest[digests.length];
      sortvals = null;
    }

    @Override
    public void resize(Resizer resizer) {
      digests = resizer.resize(digests, null);
    }
  }

  class PercentileSortedNumericAcc extends BasePercentileDVAcc {
    SortedNumericDocValues values;

    public PercentileSortedNumericAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      TDigest digest = digests[slot];
      if (digest == null) {
        digests[slot] = digest = digestSupplier.get();
      }
      for (int i = 0, count = values.docValueCount(); i < count; i++) {
        double val = getDouble(values.nextValue());
        digest.add(val);
      }
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getSortedNumeric(readerContext.reader(),  sf.getName());
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }

    /**
     * converts given long value to double based on field type
     */
    protected double getDouble(long val) {
      switch (sf.getType().getNumberType()) {
        case INTEGER:
        case LONG:
        case DATE:
          return val;
        case FLOAT:
          return NumericUtils.sortableIntToFloat((int) val);
        case DOUBLE:
          return NumericUtils.sortableLongToDouble(val);
        default:
          // this would never happen
          return 0.0d;
      }
    }
  }

  class PercentileSortedSetAcc extends BasePercentileDVAcc {
    SortedSetDocValues values;

    public PercentileSortedSetAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
    }

    @Override
    protected void collectValues(int doc, int slot) throws IOException {
      TDigest digest = digests[slot];
      if (digest == null) {
        digests[slot] = digest = digestSupplier.get();
      }
      long ord;
      while ((ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        BytesRef term = values.lookupOrd(ord);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date ? ((Date)obj).getTime(): ((Number)obj).doubleValue();
        digest.add(val);
      }
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      super.setNextReader(readerContext);
      values = DocValues.getSortedSet(readerContext.reader(),  sf.getName());
    }

    @Override
    protected boolean advanceExact(int doc) throws IOException {
      return values.advanceExact(doc);
    }
  }

  class PercentileUnInvertedFieldAcc extends UnInvertedFieldAcc {
    protected TDigest[] digests;
    protected ByteBuffer buf;
    protected double[] sortvals;
    private int currentSlot;

    public PercentileUnInvertedFieldAcc(FacetContext fcontext, SchemaField sf, int numSlots) throws IOException {
      super(fcontext, sf, numSlots);
      digests = new TDigest[numSlots];
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      this.currentSlot = slot;
      docToTerm.getBigTerms(doc + currentDocBase, this);
      docToTerm.getSmallTerms(doc + currentDocBase, this);
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
      for (int i = 0; i < digests.length; i++) {
        TDigest digest = digests[i];
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
      TDigest digest = digests[slot];
      if (digest == null) return null;

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
      digests = new TDigest[digests.length];
      sortvals = null;
    }

    @Override
    public void resize(Resizer resizer) {
      digests = resizer.resize(digests, null);
    }

    @Override
    public void call(int ord) {
      TDigest digest = digests[currentSlot];
      if (digest == null) {
        digests[currentSlot] = digest = digestSupplier.get();
      }
      try {
        BytesRef term = docToTerm.lookupOrd(ord);
        Object obj = sf.getType().toObject(sf, term);
        double val = obj instanceof Date ? ((Date) obj).getTime() : ((Number) obj).doubleValue();
        digest.add(val);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  class Merger extends FacetModule.FacetSortableMerger {
    protected TDigest digest;
    protected Double sortVal;

    @Override
    public void merge(Object facetResult, Context mcontext) {
      byte[] arr = (byte[])facetResult;
      if (arr == null) return; // an explicit null can mean no values in the field
      TDigest subDigest = digestParser.apply(ByteBuffer.wrap(arr));
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
    public Object getPrototype() {
      return 0d;
    }

    @Override
    public void readState(JavaBinDecoder codec, Context mcontext) throws IOException {
      byte[] arr = (byte[]) codec.readVal();
      digest = digestParser.apply(ByteBuffer.wrap(arr));
    }

    @Override
    public void writeState(JavaBinCodec codec) throws IOException {
      digest.compress();
      int sz = digest.byteSize();
      ByteBuffer buf = ByteBuffer.allocate(sz + (sz >> 1));  // oversize by 50%
      digest.asSmallBytes(buf);
      byte[] arr = Arrays.copyOf(buf.array(), buf.position());
      codec.writeVal(arr);
    }

    @Override
    public int compareTo(FacetModule.FacetSortableMerger other, FacetRequest.SortDirection direction) {
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
