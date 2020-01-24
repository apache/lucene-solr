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
package org.apache.solr.analytics.function.reduction.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.solr.analytics.stream.reservation.DoubleArrayReservation;
import org.apache.solr.analytics.stream.reservation.FloatArrayReservation;
import org.apache.solr.analytics.stream.reservation.IntArrayReservation;
import org.apache.solr.analytics.stream.reservation.LongArrayReservation;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.stream.reservation.StringArrayReservation;
import org.apache.solr.analytics.util.OrdinalCalculator;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValueStream;

/**
 * Collector of sorted lists.
 *
 * Once the sorted list has been collected, it can be reduced by calculating a median, percentiles, or ordinals.
 * All of the above reductions over the same data share one {@link SortedListCollector}.
 * <p>
 * Supported types are:
 * <ul>
 * <li>Int
 * <li>Long
 * <li>Float
 * <li>Double
 * <li>Date (through longs)
 * <li>String
 * </ul>
 *
 * @param <T> The type of data being processed.
 */
public abstract class SortedListCollector<T extends Comparable<T>> extends ReductionDataCollector<SortedListCollector.SortedListData<T>> {
  public static final String name = "sorted";
  private final String exprStr;

  protected SortedListCollector(AnalyticsValueStream param, String specificationName) {
    this.exprStr = AnalyticsValueStream.createExpressionString(name + "_" + specificationName,param);

    tempList = new ArrayList<>();

    calcMedian = false;
    calcPercs = new HashSet<>();
    calcOrds = new HashSet<>();
  }

  private List<T> list;

  private boolean calcMedian;
  private Set<Double> calcPercs;
  private Set<Integer> calcOrds;

  public int size() {
    return list.size();
  }

  /**
   * Informs the collector that the median needs to be computed.
   */
  public void calcMedian() {
    calcMedian = true;
  }

  /**
   * Informs the collector that the following percentile needs to be computed.
   *
   * @param percentile requested percentile
   */
  public void calcPercentile(double percentile) {
    calcPercs.add(percentile);
  }

  /**
   * Informs the collector that the following ordinal needs to be computed.
   *
   * @param ordinal requested ordinal
   */
  public void calcOrdinal(int ordinal) {
    calcOrds.add(ordinal);
  }

  /**
   * Once the data has been set by either {@link #setData} or {@link #setMergedData},
   * this returns the value at the given sorted index.
   *
   * Only the indices specified by {@link #calcMedian}, {@link #calcPercentile}, and {@link #calcOrdinal}
   * will contain valid data. All other indices may return unsorted data.
   *
   * @param index the index of the sorted data to return
   */
  public T get(int index) {
    return list.get(index);
  }

  @Override
  public SortedListData<T> newData() {
    SortedListData<T> data = new SortedListData<>();
    data.list = new ArrayList<T>();
    data.exists = false;
    return data;
  }

  ArrayList<T> tempList;
  @Override
  protected void apply(SortedListData<T> data) {
    data.list.addAll(tempList);
  }

  /**
   * Starts the import of the shard data.
   *
   * @param size the size of the incoming shard list
   */
  protected void startImport(int size) {
    ioData.list.ensureCapacity(ioData.list.size() + size);
  }

  /**
   * Merges the current list with the incoming value.
   *
   * @param value the next imported value to add
   */
  protected void importNext(T value) {
    ioData.list.add(value);
  }

  Iterator<T> iter;
  /**
   * The list to be exported is unsorted.
   * The lists of all shards will be combined with the {@link #startImport} and {@link #importNext} methods.
   *
   * @return the size of the list being exported.
   */
  public int startExport() {
    iter = ioData.list.iterator();
    return ioData.list.size();
  }
  /**
   * Return the next value in the list.
   *
   * @return the next sorted value
   */
  public T exportNext() {
    return iter.next();
  }

  /**
   * Put the given indices in their sorted positions
   */
  @Override
  public void setMergedData(ReductionData data) {
    setData(data);
  }

  /**
   * This is where the given indices are put in their sorted positions.
   *
   * Only the given indices are guaranteed to be in sorted order.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void setData(ReductionData data) {
    list = ((SortedListData<T>)data).list;
    int size = list.size();
    if (size <= 1) {
      return;
    }

    // Ordinals start at 0 and end at size-1
    Set<Integer> ordinals = new HashSet<>();
    for (double percentile : calcPercs) {
      ordinals.add((int) Math.round(percentile * size - .5));
    }
    for (int ordinal : calcOrds) {
      if (ordinal > 0) {
        ordinals.add(ordinal - 1);
      } else if (ordinal < 0){
        ordinals.add(size + ordinal);
      }
    }
    if (calcMedian) {
      int mid = list.size() / 2;
      ordinals.add(mid);
      if (list.size() % 2 == 0) {
        ordinals.add(mid - 1);
      }
    }
    OrdinalCalculator.putOrdinalsInPosition(list, ordinals);
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }

  public static class SortedListData<D extends Comparable<D>> extends ReductionData {
    ArrayList<D> list;
  }

  public static class SortedIntListCollector extends SortedListCollector<Integer> {
    private IntValueStream param;

    public SortedIntListCollector(IntValueStream param) {
      super(param, "int");
      this.param = param;
    }

    @Override
    public void collect() {
      tempList.clear();
      param.streamInts( val -> tempList.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new IntArrayReservation(
          value -> importNext(value),
          importSize -> startImport(importSize),
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class SortedLongListCollector extends SortedListCollector<Long> {
    private LongValueStream param;

    public SortedLongListCollector(LongValueStream param) {
      super(param, "long");
      this.param = param;
    }

    @Override
    public void collect() {
      tempList.clear();
      param.streamLongs( val -> tempList.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new LongArrayReservation(
          value -> importNext(value),
          importSize -> startImport(importSize),
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class SortedFloatListCollector extends SortedListCollector<Float> {
    private FloatValueStream param;

    public SortedFloatListCollector(FloatValueStream param) {
      super(param, "float");
      this.param = param;
    }

    @Override
    public void collect() {
      tempList.clear();
      param.streamFloats( val -> tempList.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new FloatArrayReservation(
          value -> importNext(value),
          importSize -> startImport(importSize),
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class SortedDoubleListCollector extends SortedListCollector<Double> {
    private DoubleValueStream param;

    public SortedDoubleListCollector(DoubleValueStream param) {
      super(param, "double");
      this.param = param;
    }

    @Override
    public void collect() {
      tempList.clear();
      param.streamDoubles( val -> tempList.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new DoubleArrayReservation(
          value -> importNext(value),
          importSize -> startImport(importSize),
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class SortedStringListCollector extends SortedListCollector<String> {
    private StringValueStream param;

    public SortedStringListCollector(StringValueStream param) {
      super(param, "string");
      this.param = param;
    }

    @Override
    public void collect() {
      tempList.clear();
      param.streamStrings( val -> tempList.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new StringArrayReservation(
          value -> importNext(value),
          importSize -> startImport(importSize),
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }
}