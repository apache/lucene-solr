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

import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.schema.SchemaField;

import java.util.Locale;


/**
 * Contains all of the specifications for a field facet.
 */
public class FieldFacetRequest extends AbstractFieldFacetRequest {

  private FacetSortSpecification sort = null;
  private FacetSortDirection dir = null;
  private int limit;
  private int offset;
  private boolean missing;
  private boolean hidden;
  
  
  public static enum FacetSortDirection {
    ASCENDING ,
    DESCENDING;
   
    public static FacetSortDirection fromExternal(String value){
      final String sort = value.toLowerCase(Locale.ROOT);
      if( "asc".equals(sort) )            return ASCENDING;
      if( "ascending".equals(sort) )      return ASCENDING;
      if( "desc".equals(sort) )           return DESCENDING;
      if( "descending".equals(sort) )     return DESCENDING;
      return Enum.valueOf(FacetSortDirection.class, value);
    }
  }
  
  /**
   * Specifies how to sort the buckets of a field facet.
   * 
   */
  public static class FacetSortSpecification {
    private String statistic;
    private FacetSortDirection direction = FacetSortDirection.DESCENDING;
    
    public FacetSortSpecification(){}
    
    /**
     * @param statistic The name of a statistic specified in the {@link AnalyticsRequest}
     * which is wrapping the {@link FieldFacetRequest} being sorted.
     */
    public FacetSortSpecification(String statistic) {
      this.statistic = statistic;
    }
    
    public FacetSortSpecification(String statistic, FacetSortDirection direction) {
      this(statistic);
      this.direction = direction;
    }
    
    public String getStatistic() {
      return statistic;
    }
    public void setStatistic(String statistic) {
      this.statistic = statistic;
    }
    public FacetSortDirection getDirection() {
      return direction;
    }
    public void setDirection(FacetSortDirection direction) {
      this.direction = direction;
    }

    public static FacetSortSpecification fromExternal(String spec){
      String[] parts = spec.split(" ",2);
      if( parts.length == 1 ){
        return new FacetSortSpecification(parts[0]);
      } else {
        return new FacetSortSpecification(parts[0], FacetSortDirection.fromExternal(parts[1]));
      }
    }

    @Override
    public String toString() {
      return "<SortSpec stat=" + statistic + " dir=" + direction + ">";
    }
  }

  public FieldFacetRequest(SchemaField field) {
    super(field);
    this.limit = AnalyticsParams.DEFAULT_LIMIT;
    this.hidden = AnalyticsParams.DEFAULT_HIDDEN;
  }

  public FacetSortDirection getDirection() {
    return dir;
  }

  public void setDirection(String dir) {
    this.dir = FacetSortDirection.fromExternal(dir);
    if (sort!=null) {
      sort.setDirection(this.dir);
    }
  }

  public FacetSortSpecification getSort() {
    return sort;
  }

  public void setSort(FacetSortSpecification sort) {
    this.sort = sort;
  }

  public boolean showsMissing() {
    return missing;
  }

  /**
   * If there are missing values in the facet field, include the bucket 
   * for the missing facet values in the facet response.
   * @param missing true/false if we calculate missing
   */
  public void showMissing(boolean missing) {
    this.missing = missing;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }
  
  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isHidden() {
    return hidden;
  }

  public void setHidden(boolean hidden) {
    this.hidden = hidden;
  }

  @Override
  public String toString() {
    return "<FieldFacetRequest field="+field.getName()+(sort==null?"":" sort=" + sort) + " limit=" + limit+" offset="+offset+">";
  }


  
}
