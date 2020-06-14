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
package org.apache.solr.client.solrj.response;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a range facet result
 */
public abstract class RangeFacet<B, G> {

  private final String name;
  private final List<Count> counts = new ArrayList<>();

  private final B start;
  private final B end;
  private final G gap;

  private final Number before;
  private final Number after;
  private final Number between;

  protected RangeFacet(String name, B start, B end, G gap, Number before, Number after, Number between) {
    this.name = name;
    this.start = start;
    this.end = end;
    this.gap = gap;
    this.before = before;
    this.after = after;
    this.between = between;
  }

  public void addCount(String value, int count) {
    counts.add(new Count(value, count, this));
  }

  public String getName() {
    return name;
  }

  public List<Count> getCounts() {
    return counts;
  }

  public B getStart() {
    return start;
  }

  public B getEnd() {
    return end;
  }

  public G getGap() {
    return gap;
  }

  public Number getBefore() {
    return before;
  }

  public Number getAfter() {
    return after;
  }

  public Number getBetween() {
    return between;
  }

  public static class Numeric extends RangeFacet<Number, Number> {

    public Numeric(String name, Number start, Number end, Number gap, Number before, Number after, Number between) {
      super(name, start, end, gap, before, after, between);
    }

  }

  public static class Date extends RangeFacet<java.util.Date, String> {

    public Date(String name, java.util.Date start, java.util.Date end, String gap, Number before, Number after, Number between) {
      super(name, start, end, gap, before, after, between);
    }

  }

  public static class Currency extends RangeFacet<String, String> {
    public Currency(String name, String start, String end, String gap, Number before, Number after, Number between) {
      super(name, start, end, gap, before, after, between);
    }
  }

  public static class Count {

    private final String value;
    private final int count;
    @SuppressWarnings({"rawtypes"})
    private final RangeFacet rangeFacet;

    public Count(String value, int count,
                 @SuppressWarnings({"rawtypes"})RangeFacet rangeFacet) {
      this.value = value;
      this.count = count;
      this.rangeFacet = rangeFacet;
    }

    public String getValue() {
      return value;
    }

    public int getCount() {
      return count;
    }

    @SuppressWarnings({"rawtypes"})
    public RangeFacet getRangeFacet() {
      return rangeFacet;
    }
  }

}
