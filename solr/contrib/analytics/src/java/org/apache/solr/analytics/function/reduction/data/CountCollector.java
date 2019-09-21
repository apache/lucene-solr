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

import java.util.function.Consumer;

import org.apache.solr.analytics.stream.reservation.LongReservation;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.value.AnalyticsValueStream;

public abstract class CountCollector extends ReductionDataCollector<CountCollector.CountData> {
  public static final String name = "count";
  private final String exprStr;

  public CountCollector(String exprStr) {
    this.exprStr = exprStr;
  }

  private long count;
  private long docCount;

  /**
   * The number of Solr Documents for which the given analytics expression exists.
   *
   * @return the count
   */
  public long count() {
    return count;
  }
  /**
   * The number of Solr Documents used in this reduction.
   *
   * @return the number of documents
   */
  public long docCount() {
    return docCount;
  }

  @Override
  public CountData newData() {
    CountData data = new CountData();
    data.count = 0;
    data.missing = 0;
    data.docCount = 0;
    return data;
  }

  @Override
  public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
    // Count
    consumer.accept(new LongReservation(
        value -> ioData.count += value,
        () -> ioData.count
      ));
    // DocCount
    consumer.accept(new LongReservation(
        value -> ioData.docCount += value,
        () -> ioData.docCount
      ));
  }

  @Override
  public void setMergedData(ReductionData data) {
    count = ((CountData)data).count;
    docCount = ((CountData)data).docCount;
  }

  @Override
  public void setData(ReductionData data) {
    count = ((CountData)data).count;
    docCount = ((CountData)data).docCount;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }

  public static class CountData extends ReductionData {
    long count;
    long missing;
    long docCount;
  }

  /**
   * Represents a {@code count(expr)} expression. This collects 3 values:
   *
   * docCount - The number of Solr Documents for which the wrapped expression exists.
   * count - The number of values which wrapped expression contains.
   * missing - The number of Solr Documents for which the wrapped expression does not exist.
   */
  public static class ExpressionCountCollector extends CountCollector {
    private final AnalyticsValueStream param;

    public ExpressionCountCollector(AnalyticsValueStream param) {
      super(AnalyticsValueStream.createExpressionString(name, param));
      this.param = param;
    }

    private long missing;

    /**
     * The number of Solr Documents for which the given analytics expression does not exist.
     *
     * @return the number of missing values
     */
    public long missing() {
      return missing;
    }

    @Override
    public void setMergedData(ReductionData data) {
      super.setMergedData(data);
      missing = ((CountData)data).missing;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      missing = ((CountData)data).missing;
    }

    long tempCount;
    int tempMissing;
    int tempDocCount;
    @Override
    public void collect() {
      tempCount = 0;
      param.streamObjects( obj -> {
        ++tempCount;
      });
      tempMissing = tempCount == 0 ? 1 : 0;
      tempDocCount = tempCount > 0 ? 1 : 0;
    }

    @Override
    protected void apply(CountData data) {
      data.count += tempCount;
      data.missing += tempMissing;
      data.docCount += tempDocCount;
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      super.submitReservations(consumer);
      // Missing
      consumer.accept(new LongReservation(
          value -> ioData.missing += value,
          () -> ioData.missing
        ));
    }
  }

  /**
   * Represents a {@code count()} expression. This collects the number of Solr Documents used in a result set.
   */
  public static class TotalCountCollector extends CountCollector {

    public TotalCountCollector() {
      super(AnalyticsValueStream.createExpressionString(name));
    }

    @Override
    protected void apply(CountData data) {
      data.count += 1;
      data.docCount += 1;
    }
  }
}