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
import org.apache.solr.analytics.stream.reservation.FloatCheckedReservation;
import org.apache.solr.analytics.stream.reservation.IntCheckedReservation;
import org.apache.solr.analytics.stream.reservation.LongCheckedReservation;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.stream.reservation.StringCheckedReservation;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.DoubleValueStream;
import org.apache.solr.analytics.value.FloatValueStream;
import org.apache.solr.analytics.value.IntValueStream;
import org.apache.solr.analytics.value.LongValueStream;
import org.apache.solr.analytics.value.StringValueStream;

/**
 * Collector of max values.
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
public abstract class MaxCollector<T extends ReductionData> extends ReductionDataCollector<T> {
  public static final String name = "max";
  private final String exprStr;

  protected MaxCollector(AnalyticsValueStream param) {
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  private boolean exists;

  /**
   * Returns true if any of the values being reduce exist, and false if none of them do.
   *
   * @return whether a max value exists
   */
  public boolean exists() {
    return exists;
  }

  @Override
  public void setMergedData(ReductionData data) {
    exists = data.exists;
  }

  @Override
  public void setData(ReductionData data) {
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

  public static class IntMaxCollector extends MaxCollector<IntMaxCollector.MaxData> {
    private IntValueStream param;

    public IntMaxCollector(IntValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MaxData newData() {
      MaxData data = new MaxData();
      data.exists = false;
      return data;
    }

    int max;

    /**
     * Returns the max value of the set data.
     *
     * @return the max
     */
    public int max() {
      return max;
    }

    int tempMax;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamInts( val -> {
        if (!tempExists || val > tempMax) {
          tempMax = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MaxData data) {
      if (tempExists && (!data.exists || tempMax > data.val)) {
        data.val = tempMax;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new IntCheckedReservation(
          value -> {
            if (!ioData.exists || value > ioData.val) {
              ioData.val = value;
              ioData.exists = true;
            }
          },
          ()-> ioData.val,
          ()-> ioData.exists
        ));
    }

    @Override
    public void setMergedData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    public static class MaxData extends ReductionData {
      int val;
    }
  }



  public static class LongMaxCollector extends MaxCollector<LongMaxCollector.MaxData> {
    private LongValueStream param;

    public LongMaxCollector(LongValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MaxData newData() {
      MaxData data = new MaxData();
      data.exists = false;
      return data;
    }

    long max;

    /**
     * Returns the max value of the set data.
     *
     * @return the max
     */
    public long max() {
      return max;
    }

    long tempMax;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamLongs( val -> {
        if (!tempExists || val > tempMax) {
          tempMax = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MaxData data) {
      if (tempExists && (!data.exists || tempMax > data.val)) {
        data.val = tempMax;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new LongCheckedReservation(
          value -> {
            if (!ioData.exists || value > ioData.val) {
              ioData.val = value;
              ioData.exists = true;
            }
          },
          ()-> ioData.val,
          ()-> ioData.exists
        ));
    }

    @Override
    public void setMergedData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    public static class MaxData extends ReductionData {
      long val;
    }
  }

  public static class FloatMaxCollector extends MaxCollector<FloatMaxCollector.MaxData> {
    private FloatValueStream param;

    public FloatMaxCollector(FloatValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MaxData newData() {
      MaxData data = new MaxData();
      data.exists = false;
      return data;
    }

    float max;

    /**
     * Returns the max value of the set data.
     *
     * @return the max
     */
    public float max() {
      return max;
    }

    float tempMax;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamFloats( val -> {
        if (!tempExists || val > tempMax) {
          tempMax = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MaxData data) {
      if (tempExists && (!data.exists || tempMax > data.val)) {
        data.val = tempMax;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new FloatCheckedReservation(
          value -> {
            if (!ioData.exists || value > ioData.val) {
              ioData.val = value;
              ioData.exists = true;
            }
          },
          ()-> ioData.val,
          ()-> ioData.exists
        ));
    }

    @Override
    public void setMergedData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    public static class MaxData extends ReductionData {
      float val;
    }
  }

  public static class DoubleMaxCollector extends MaxCollector<DoubleMaxCollector.MaxData> {
    private DoubleValueStream param;

    public DoubleMaxCollector(DoubleValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MaxData newData() {
      MaxData data = new MaxData();
      data.exists = false;
      return data;
    }

    double max;

    /**
     * Returns the max value of the set data.
     *
     * @return the max
     */
    public double max() {
      return max;
    }

    double tempMax;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamDoubles( val -> {
        if (!tempExists || val > tempMax) {
          tempMax = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MaxData data) {
      if (tempExists && (!data.exists || tempMax > data.val)) {
        data.val = tempMax;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new DoubleCheckedReservation(
          value -> {
            if (!ioData.exists || value > ioData.val) {
              ioData.val = value;
              ioData.exists = true;
            }
          },
          ()-> ioData.val,
          ()-> ioData.exists
        ));
    }

    @Override
    public void setMergedData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    public static class MaxData extends ReductionData {
      double val;
    }
  }



  public static class StringMaxCollector extends MaxCollector<StringMaxCollector.MaxData> {
    private StringValueStream param;

    public StringMaxCollector(StringValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MaxData newData() {
      MaxData data = new MaxData();
      data.exists = false;
      return data;
    }

    String max;

    /**
     * Returns the max value of the set data.
     *
     * @return the max
     */
    public String max() {
      return max;
    }

    String tempMax;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamStrings( val -> {
        if (!tempExists || val.compareTo(tempMax) > 0) {
          tempMax = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MaxData data) {
      if (tempExists && (!data.exists || tempMax.compareTo(data.val) > 0)) {
        data.val = tempMax;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new StringCheckedReservation(
          value -> {
            if (!ioData.exists || value.compareTo(ioData.val) > 0) {
              ioData.val = value;
              ioData.exists = true;
            }
          },
          ()-> ioData.val,
          ()-> ioData.exists
        ));
    }

    @Override
    public void setMergedData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      max = ((MaxData)data).val;
    }

    public static class MaxData extends ReductionData {
      String val;
    }
  }
}