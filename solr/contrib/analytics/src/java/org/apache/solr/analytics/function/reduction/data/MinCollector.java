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
 * Collector of min values.
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
public abstract class MinCollector<T extends ReductionData> extends ReductionDataCollector<T> {
  public static final String name = "min";
  private final String exprStr;

  protected MinCollector(AnalyticsValueStream param) {
    this.exprStr = AnalyticsValueStream.createExpressionString(name,param);
  }

  private boolean exists;

  /**
   * Returns true if any of the values being reduce exist, and false if none of them do.
   *
   * @return whether a min value exists
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

  public static class IntMinCollector extends MinCollector<IntMinCollector.MinData> {
    private IntValueStream param;

    public IntMinCollector(IntValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MinData newData() {
      MinData data = new MinData();
      data.exists = false;
      return data;
    }

    int min;

    /**
     * Returns the min value of the set data.
     *
     * @return the min
     */
    public int min() {
      return min;
    }

    int tempMin;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamInts( val -> {
        if (!tempExists || val < tempMin) {
          tempMin = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MinData data) {
      if (tempExists && (!data.exists || tempMin < data.val)) {
        data.val = tempMin;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new IntCheckedReservation(
          value -> {
            if (!ioData.exists || value < ioData.val) {
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
      min = ((MinData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      min = ((MinData)data).val;
    }

    public static class MinData extends ReductionData {
      int val;
    }
  }



  public static class LongMinCollector extends MinCollector<LongMinCollector.MinData> {
    private LongValueStream param;

    public LongMinCollector(LongValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MinData newData() {
      MinData data = new MinData();
      data.exists = false;
      return data;
    }

    long min;

    /**
     * Returns the min value of the set data.
     *
     * @return the min
     */
    public long min() {
      return min;
    }

    long tempMin;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamLongs( val -> {
        if (!tempExists || val < tempMin) {
          tempMin = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MinData data) {
      if (tempExists && (!data.exists || tempMin < data.val)) {
        data.val = tempMin;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new LongCheckedReservation(
          value -> {
            if (!ioData.exists || value < ioData.val) {
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
      min = ((MinData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      min = ((MinData)data).val;
    }

    public static class MinData extends ReductionData {
      long val;
    }
  }

  public static class FloatMinCollector extends MinCollector<FloatMinCollector.MinData> {
    private FloatValueStream param;

    public FloatMinCollector(FloatValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MinData newData() {
      MinData data = new MinData();
      data.exists = false;
      return data;
    }

    float min;

    /**
     * Returns the min value of the set data.
     *
     * @return the min
     */
    public float min() {
      return min;
    }

    float tempMin;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamFloats( val -> {
        if (!tempExists || val < tempMin) {
          tempMin = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MinData data) {
      if (tempExists && (!data.exists || tempMin < data.val)) {
        data.val = tempMin;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new FloatCheckedReservation(
          value -> {
            if (!ioData.exists || value < ioData.val) {
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
      min = ((MinData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      min = ((MinData)data).val;
    }

    public static class MinData extends ReductionData {
      float val;
    }
  }

  public static class DoubleMinCollector extends MinCollector<DoubleMinCollector.MinData> {
    private DoubleValueStream param;

    public DoubleMinCollector(DoubleValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MinData newData() {
      MinData data = new MinData();
      data.exists = false;
      return data;
    }

    double min;

    /**
     * Returns the min value of the set data.
     *
     * @return the min
     */
    public double min() {
      return min;
    }

    double tempMin;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamDoubles( val -> {
        if (!tempExists || val < tempMin) {
          tempMin = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MinData data) {
      if (tempExists && (!data.exists || tempMin < data.val)) {
        data.val = tempMin;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new DoubleCheckedReservation(
          value -> {
            if (!ioData.exists || value < ioData.val) {
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
      min = ((MinData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      min = ((MinData)data).val;
    }

    public static class MinData extends ReductionData {
      double val;
    }
  }



  public static class StringMinCollector extends MinCollector<StringMinCollector.MinData> {
    private StringValueStream param;

    public StringMinCollector(StringValueStream param) {
      super(param);
      this.param = param;
    }

    @Override
    public MinData newData() {
      MinData data = new MinData();
      data.exists = false;
      return data;
    }

    String min;

    /**
     * Returns the min value of the set data.
     *
     * @return the min
     */
    public String min() {
      return min;
    }

    String tempMin;
    boolean tempExists;
    @Override
    public void collect() {
      tempExists = false;
      param.streamStrings( val -> {
        if (!tempExists || val.compareTo(tempMin) < 0) {
          tempMin = val;
          tempExists = true;
        }
      });
    }
    @Override
    protected void apply(MinData data) {
      if (tempExists && (!data.exists || tempMin.compareTo(data.val) < 0)) {
        data.val = tempMin;
        data.exists = true;
      }
    }

    @Override
    public void submitReservations(Consumer<ReductionDataReservation<?,?>> consumer) {
      consumer.accept(new StringCheckedReservation(
          value -> {
            if (!ioData.exists || value.compareTo(ioData.val) < 0) {
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
      min = ((MinData)data).val;
    }

    @Override
    public void setData(ReductionData data) {
      super.setData(data);
      min = ((MinData)data).val;
    }

    public static class MinData extends ReductionData {
      String val;
    }
  }
}