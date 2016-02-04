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
package org.apache.solr.analytics.request;

import java.util.Arrays;
import java.util.EnumSet;

import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.schema.SchemaField;

/**
 * Contains all of the specifications for a range facet.
 */
public class RangeFacetRequest extends AbstractFieldFacetRequest {
  protected String start;
  protected String end;
  protected String[] gaps;
  protected boolean hardEnd = false;
  protected EnumSet<FacetRangeInclude> include;
  protected boolean includeCalled = false;
  protected EnumSet<FacetRangeOther> others;
  protected boolean othersCalled = false;
  
  public RangeFacetRequest(SchemaField field) {
    super(field);
    include = EnumSet.of(AnalyticsParams.DEFAULT_INCLUDE);
    others = EnumSet.of(AnalyticsParams.DEFAULT_OTHER);
  }
  
  public RangeFacetRequest(SchemaField field, String start, String end, String[] gaps) {
    super(field);
    this.start = start;
    this.end = end;
    this.gaps = gaps;
  }

  public String getStart() {
    return start;
  }

  public void setStart(String start) {
    this.start = start;
  }

  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }

  public EnumSet<FacetRangeInclude> getInclude() {
    return include;
  }

  public void setInclude(EnumSet<FacetRangeInclude> include) {
    includeCalled = true;
    this.include = include;
  }

  public void addInclude(FacetRangeInclude include) {
    if (includeCalled) {
      this.include.add(include);
    } else {
      includeCalled = true;
      this.include = EnumSet.of(include);
    }
  }

  public String[] getGaps() {
    return gaps;
  }

  public void setGaps(String[] gaps) {
    this.gaps = gaps;
  }

  public boolean isHardEnd() {
    return hardEnd;
  }

  public void setHardEnd(boolean hardEnd) {
    this.hardEnd = hardEnd;
  }

  public EnumSet<FacetRangeOther> getOthers() {
    return others;
  }

  public void setOthers(EnumSet<FacetRangeOther> others) {
    othersCalled = true;
    this.others = others;
  }

  public void addOther(FacetRangeOther other) {
    if (othersCalled) {
      this.others.add(other);
    } else {
      othersCalled = true;
      this.others = EnumSet.of(other);
    }
  }

  @Override
  public String toString() {
    return "<RangeFacetRequest field="+field.getName() + " start=" + start + ", end=" + end + ", gap=" + Arrays.toString(gaps) + ", hardEnd=" + hardEnd + 
                               ", include=" + include + ", others=" + others +">";
  }

  
  
}
