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

import org.apache.solr.analytics.stream.reservation.DoubleCheckedReservation;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;

/**
 * Collects the sum of the given {@link DoubleValueStream} parameter.
 */
public class SumCollector extends ReductionDataCollector<SumCollector.SumData> {
  private final DoubleValueStream param;
  public static final String name = "sum";
  private final String exprStr;

  public SumCollector(DoubleValueStream param) {
    this.param = param;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  private double sum;
  private boolean exists;

  /**
   * Return the sum of the set data
   *
   * @return the sum
   */
  public double sum() {
    return sum;
  }

  /**
   * Return whether a sum exists.
   * A sum will always exist if there is at least one existing value for the parameter,
   * otherwise the sum does not exist.
   *
   * @return whether a sum exists
   */
  public boolean exists() {
    return exists;
  }

  @Override
  public SumData newData() {
    SumData data = new SumData();
    data.sum = 0;
    data.exists = false;
    return data;
  }

  double tempSum;
  boolean tempExists;
  @Override
  public void collect() {
    tempSum = 0;
    tempExists = false;
    param.streamDoubles( val -> {
      tempSum += val;
      tempExists = true;
    });
  }
  @Override
  protected void apply(SumData data) {
    data.sum += tempSum;
    data.exists |= tempExists;
  }

  @Override
  public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
    consumer.accept(new DoubleCheckedReservation(
        value -> {
          ioData.sum += value;
          ioData.exists = true;
        },
        ()-> ioData.sum,
        ()-> ioData.exists
      ));
  }

  @Override
  public void setMergedData(ReductionData data) {
    sum = ((SumData)data).sum;
    exists = data.exists;
  }

  @Override
  public void setData(ReductionData data) {
    sum = ((SumData)data).sum;
    exists = data.exists;
  }

  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }

  public static class SumData extends ReductionData {
    double sum;
  }
}