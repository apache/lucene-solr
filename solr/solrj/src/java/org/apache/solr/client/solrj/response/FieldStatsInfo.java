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

import org.apache.solr.common.util.NamedList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds stats info
 *
 *
 * @since solr 1.4
 */
public class FieldStatsInfo implements Serializable {
  final String name;
  
  Object min;
  Object max;
  Object sum;
  Long count;
  Long countDistinct;
  Collection<Object> distinctValues;
  Long missing;
  Object mean = null;
  Double sumOfSquares = null;
  Double stddev = null;
  Long cardinality = null;
  
  Map<String,List<FieldStatsInfo>> facets;
  
  Map<Double, Double> percentiles;
  
  @SuppressWarnings({"unchecked"})
  public FieldStatsInfo( NamedList<Object> nl, String fname )
  {
    name = fname;
    
    for( Map.Entry<String, Object> entry : nl ) {
      if( "min".equals( entry.getKey() ) ) {
        min = entry.getValue();
      }
      else if( "max".equals( entry.getKey() ) ) {
        max = entry.getValue();
      }
      else if( "sum".equals( entry.getKey() ) ) {
        sum = entry.getValue();
      }
      else if( "count".equals( entry.getKey() ) ) {
        count = (Long)entry.getValue();
      }
      else if ("countDistinct".equals(entry.getKey())) {
        countDistinct = (Long) entry.getValue();
      }
      else if ("distinctValues".equals(entry.getKey())) {
        distinctValues = (Collection<Object>) entry.getValue();
      }
      else if( "missing".equals( entry.getKey() ) ) {
        missing = (Long)entry.getValue();
      }
      else if( "mean".equals( entry.getKey() ) ) {
        mean = entry.getValue();
      }
      else if( "sumOfSquares".equals( entry.getKey() ) ) {
        sumOfSquares = (Double)entry.getValue();
      }
      else if( "stddev".equals( entry.getKey() ) ) {
        stddev = (Double)entry.getValue();
      }
      else if( "facets".equals( entry.getKey() ) ) {
        @SuppressWarnings("unchecked")
        NamedList<Object> fields = (NamedList<Object>)entry.getValue();
        facets = new HashMap<>();
        for( Map.Entry<String, Object> ev : fields ) {
          List<FieldStatsInfo> vals = new ArrayList<>();
          facets.put( ev.getKey(), vals );
          @SuppressWarnings("unchecked")
          NamedList<NamedList<Object>> vnl = (NamedList<NamedList<Object>>) ev.getValue();
          for( int i=0; i<vnl.size(); i++ ) {
            String n = vnl.getName(i);
            vals.add( new FieldStatsInfo( vnl.getVal(i), n ) );
          }
        }
      } else if ( "percentiles".equals( entry.getKey() ) ){
        @SuppressWarnings("unchecked")
        NamedList<Object> fields = (NamedList<Object>) entry.getValue();
        percentiles = new LinkedHashMap<>();
        for( Map.Entry<String, Object> ev : fields ) {
          percentiles.put(Double.parseDouble(ev.getKey()), (Double)ev.getValue());
        }
      } else if ( "cardinality".equals(entry.getKey()) ) {
        cardinality = (Long)entry.getValue();
      }
      else {
        throw new RuntimeException( "unknown key: "+entry.getKey() + " ["+entry.getValue()+"]" );
      }
    }
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append( name );
    sb.append( ": {" );
    if( min != null ) {
      sb.append( " min:").append( min );
    }
    if( max != null ) {
      sb.append( " max:").append( max );
    }
    if( sum != null ) {
      sb.append( " sum:").append( sum );
    }
    if( count != null ) {
      sb.append( " count:").append( count );
    }
    if (countDistinct != null) {
      sb.append(" countDistinct:").append(countDistinct);
    }
    if (distinctValues != null) {
      sb.append(" distinctValues:").append(distinctValues);
    }
    if( missing != null ) {
      sb.append( " missing:").append( missing );
    }
    if( mean != null ) {
      sb.append( " mean:").append( mean );
    }
    if( stddev != null ) {
      sb.append( " stddev:").append(stddev);
    }
    if( percentiles != null ) {
      sb.append( " percentiles:").append(percentiles);
    }
    if( cardinality != null ) {
      sb.append( " cardinality:").append(cardinality);
    }
    
    sb.append( " }" );
    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public Object getMin() {
    return min;
  }

  public Object getMax() {
    return max;
  }

  public Object getSum() {
    return sum;
  }
     
  public Long getCount() {
    return count;
  }

  public Long getCountDistinct() {
    // :TODO: as client convinience, should we return cardinality if this is null?
    return countDistinct; 
  }

  public Collection<Object> getDistinctValues() {
    return distinctValues;
  }

  public Long getMissing() {
    return missing;
  }

  public Object getMean() {
    return mean;
  }

  public Double getStddev() {
    return stddev;
  }

  public Double getSumOfSquares() {
    return sumOfSquares;
  }

  public Map<String, List<FieldStatsInfo>> getFacets() {
    return facets;
  }
  
  /**
   * The percentiles requested if any, otherwise null.  If non-null then the
   * iteration order will match the order the percentiles were originally specified in.
   */
  public Map<Double, Double> getPercentiles() {
    return percentiles;
  }

  /**
   * The cardinality of of the set of values if requested, otherwise null.
   */
  public Long getCardinality() {
    // :TODO: as client convinience, should we return countDistinct if this is null?
    return cardinality; 
  }
}
