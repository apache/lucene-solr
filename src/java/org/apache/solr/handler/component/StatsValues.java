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


package org.apache.solr.handler.component;


import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;


/** 2/11/2009 - Moved out of StatsComponent to allow open access to UnInvertedField
 * StatsValues is a utility to accumulate statistics on a set of values
 *
 * </p>
 * @see org.apache.solr.handler.component.StatsComponent
 *
*/

public class StatsValues {
  private static final String FACETS = "facets";
  double min;
  double max;
  double sum;
  double sumOfSquares;
  long count;
  long missing;
  
  // facetField   facetValue
  public Map<String, Map<String,StatsValues>> facets;
  
  public StatsValues() {
    reset();
  }

  public void accumulate(NamedList stv){
    min = Math.min(min, (Double)stv.get("min"));
    max = Math.max(max, (Double)stv.get("max"));
    sum += (Double)stv.get("sum");
    count += (Long)stv.get("count");
    missing += (Long)stv.get("missing");
    sumOfSquares += (Double)stv.get("sumOfSquares");
    
    NamedList f = (NamedList)stv.get( FACETS );
    if( f != null ) {
      if( facets == null ) {
        facets = new HashMap<String, Map<String,StatsValues>>();
      }
      
      for( int i=0; i< f.size(); i++ ) {
        String field = f.getName(i);
        NamedList vals = (NamedList)f.getVal( i );
        Map<String,StatsValues> addTo = facets.get( field );
        if( addTo == null ) {
          addTo = new HashMap<String,StatsValues>();
          facets.put( field, addTo );
        }
        for( int j=0; j< vals.size(); j++ ) {
          String val = vals.getName(j);
          StatsValues vvals = addTo.get( val );
          if( vvals == null ) {
            vvals = new StatsValues();
            addTo.put( val, vvals );
          }
          vvals.accumulate( (NamedList)vals.getVal( j ) );
        }
      }
    }
  }

  public void accumulate(double v){
    sumOfSquares += (v*v); // for std deviation
    min = Math.min(min, v);
    max = Math.max(max, v);
    sum += v;
    count++;
  }
  
  public void accumulate(double v, int c){
    sumOfSquares += (v*v*c); // for std deviation
    min = Math.min(min, v);
    max = Math.max(max, v);
    sum += v*c;
    count+= c;
  }

  public void addMissing(int c){
	missing += c;
  }
  
  public double getAverage(){
    return sum / count;
  }
  
  public double getStandardDeviation()
  {
    if( count <= 1.0D ) 
      return 0.0D;
    
    return Math.sqrt( ( ( count * sumOfSquares ) - ( sum * sum ) )
                      / ( count * ( count - 1.0D ) ) );  
  }

  public long getCount()
  {
	return count;
  }
  
  public void reset(){
    min = Double.MAX_VALUE;
    max = -1.0*Double.MAX_VALUE;
    sum = count = missing = 0;
    sumOfSquares = 0;
    facets = null;
  }
  
  public NamedList<?> getStatsValues(){
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    res.add("min", min);
    res.add("max", max);
    res.add("sum", sum);
    res.add("count", count);
    res.add("missing", missing);
    res.add("sumOfSquares", sumOfSquares );
    res.add("mean", getAverage());
    res.add( "stddev", getStandardDeviation() );
    
    // add the facet stats
    if( facets != null && facets.size() > 0 ) {
      NamedList<NamedList<?>> nl = new SimpleOrderedMap<NamedList<?>>();
      for( Map.Entry<String, Map<String,StatsValues>> entry : facets.entrySet() ) {
        NamedList<NamedList<?>> nl2 = new SimpleOrderedMap<NamedList<?>>();
        nl.add( entry.getKey(), nl2 );
        for( Map.Entry<String, StatsValues> e2 : entry.getValue().entrySet() ) {
          nl2.add( e2.getKey(), e2.getValue().getStatsValues() );
        }
      }
      res.add( FACETS, nl );
    }
    return res;
  }
}
