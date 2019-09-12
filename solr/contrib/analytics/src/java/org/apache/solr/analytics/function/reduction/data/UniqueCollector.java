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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.solr.analytics.stream.reservation.DoubleArrayReservation;
import org.apache.solr.analytics.stream.reservation.FloatArrayReservation;
import org.apache.solr.analytics.stream.reservation.IntArrayReservation;
import org.apache.solr.analytics.stream.reservation.LongArrayReservation;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.stream.reservation.StringArrayReservation;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValueStream;

/**
 * Collects the number of unique values that exist for the given parameter.
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
 */
public abstract class UniqueCollector<T> extends ReductionDataCollector<UniqueCollector.UniqueData<T>> {
  public static final String name = "unique";
  private final String exprStr;

  public UniqueCollector(AnalyticsValueStream param) {
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
    this.tempSet = new HashSet<T>();
  }

  private long count;

  /**
   * Get the count of unique values in the set data.
   *
   * @return the count of unique values
   */
  public long count() {
    return count;
  }

  @Override
  public UniqueData<T> newData() {
    UniqueData<T> data = new UniqueData<T>();
    data.set = new HashSet<>();
    data.exists = false;
    return data;
  }

  Set<T> tempSet;
  @Override
  protected void apply(UniqueData<T> data) {
    data.set.addAll(tempSet);
  }

  Iterator<T> iter;
  public int startExport() {
    iter = ioData.set.iterator();
    return ioData.set.size();
  }
  public T exportNext() {
    return iter.next();
  }

  @Override
  public void setMergedData(ReductionData data) {
    count = ((UniqueData<?>)data).set.size();
  }

  @Override
  public void setData(ReductionData data) {
    count = ((UniqueData<?>)data).set.size();
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }

  public static class UniqueData<T> extends ReductionData {
    Set<T> set;
  }

  public static class UniqueIntCollector extends UniqueCollector<Integer> {
    private IntValueStream param;

    public UniqueIntCollector(IntValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public void collect() {
      tempSet.clear();
      param.streamInts( val -> tempSet.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new IntArrayReservation(
          value -> ioData.set.add(value),
          size -> {},
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class UniqueLongCollector extends UniqueCollector<Long> {
    private LongValueStream param;

    public UniqueLongCollector(LongValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public void collect() {
      tempSet.clear();
      param.streamLongs( val -> tempSet.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new LongArrayReservation(
          value -> ioData.set.add(value),
          size -> {},
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class UniqueFloatCollector extends UniqueCollector<Float> {
    private FloatValueStream param;

    public UniqueFloatCollector(FloatValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public void collect() {
      tempSet.clear();
      param.streamFloats( val -> tempSet.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new FloatArrayReservation(
          value -> ioData.set.add(value),
          size -> {},
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class UniqueDoubleCollector extends UniqueCollector<Double> {
    private DoubleValueStream param;

    public UniqueDoubleCollector(DoubleValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public void collect() {
      tempSet.clear();
      param.streamDoubles( val -> tempSet.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new DoubleArrayReservation(
          value -> ioData.set.add(value),
          size -> {},
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }

  public static class UniqueStringCollector extends UniqueCollector<String> {
    private StringValueStream param;

    public UniqueStringCollector(StringValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public void collect() {
      tempSet.clear();
      param.streamStrings( val -> tempSet.add(val) );
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new StringArrayReservation(
          value -> ioData.set.add(value),
          size -> {},
          () -> exportNext(),
          () -> startExport()
        ));
    }
  }
}