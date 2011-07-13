/**
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds stats info
 *
 * @version $Id: SpellCheckResponse.java 693622 2008-09-09 21:21:06Z gsingers $
 * @since solr 1.4
 */
public class FieldStatsInfo implements Serializable {
  final String name;
  
  Double min;
  Double max;
  Double sum;
  Long count;
  Long missing;
  Double mean = null;
  Double sumOfSquares = null;
  Double stddev = null;
  
  Map<String,List<FieldStatsInfo>> facets;
  
  public FieldStatsInfo( NamedList<Object> nl, String fname )
  {
    name = fname;
    
    for( Map.Entry<String, Object> entry : nl ) {
      if( "min".equals( entry.getKey() ) ) {
        min = (Double)entry.getValue();
      }
      else if( "max".equals( entry.getKey() ) ) {
        max = (Double)entry.getValue();
      }
      else if( "sum".equals( entry.getKey() ) ) {
        sum = (Double)entry.getValue();
      }
      else if( "count".equals( entry.getKey() ) ) {
        count = (Long)entry.getValue();
      }
      else if( "missing".equals( entry.getKey() ) ) {
        missing = (Long)entry.getValue();
      }
      else if( "mean".equals( entry.getKey() ) ) {
        mean = (Double)entry.getValue();
      }
      else if( "sumOfSquares".equals( entry.getKey() ) ) {
        sumOfSquares = (Double)entry.getValue();
      }
      else if( "stddev".equals( entry.getKey() ) ) {
        stddev = (Double)entry.getValue();
      }
      else if( "facets".equals( entry.getKey() ) ) {
        NamedList<Object> fields = (NamedList<Object>)entry.getValue();
        facets = new HashMap<String, List<FieldStatsInfo>>();
        for( Map.Entry<String, Object> ev : fields ) {
          List<FieldStatsInfo> vals = new ArrayList<FieldStatsInfo>();
          facets.put( ev.getKey(), vals );
          NamedList<NamedList<Object>> vnl = (NamedList<NamedList<Object>>) ev.getValue();
          for( int i=0; i<vnl.size(); i++ ) {
            String n = vnl.getName(i);
            vals.add( new FieldStatsInfo( vnl.getVal(i), n ) );
          }
        }
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
    if( missing != null ) {
      sb.append( " missing:").append( missing );
    }
    if( mean != null ) {
      sb.append( " mean:").append( mean );
    }
    if( stddev != null ) {
      sb.append( " stddev:").append(stddev);
    }
    sb.append( " }" );
    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  public Double getSum() {
    return sum;
  }

  public Long getCount() {
    return count;
  }

  public Long getMissing() {
    return missing;
  }

  public Double getMean() {
    return mean;
  }

  public Double getStddev() {
    return stddev;
  }

  public Map<String, List<FieldStatsInfo>> getFacets() {
    return facets;
  }
  
}
