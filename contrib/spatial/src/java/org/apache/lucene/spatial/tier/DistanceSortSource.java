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

package org.apache.lucene.spatial.tier;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreDocComparator;
import org.apache.lucene.search.SortComparatorSource;
import org.apache.lucene.search.SortField;

/**
 * 
 * @deprecated
 * @see DistanceFieldComparatorSource
 */
public class DistanceSortSource implements SortComparatorSource {

  private static final long serialVersionUID = 1L;

  private DistanceFilter distanceFilter;
  private DistanceScoreDocLookupComparator dsdlc;
  
  public DistanceSortSource (Filter distanceFilter){

    this.distanceFilter = (DistanceFilter)distanceFilter;
    
  }
  
  public void cleanUp() {
    distanceFilter = null;
    
    if (dsdlc !=null)
      dsdlc.cleanUp();
    
    dsdlc = null;
  }
  
  public ScoreDocComparator newComparator(IndexReader reader, String field) throws IOException {
    dsdlc = new DistanceScoreDocLookupComparator(reader, distanceFilter);
    return dsdlc;
  }

  

  private class DistanceScoreDocLookupComparator implements ScoreDocComparator {

    private DistanceFilter distanceFilter;
    
    public DistanceScoreDocLookupComparator(IndexReader reader, DistanceFilter distanceFilter) {
      this.distanceFilter = distanceFilter;
      return;
    }
    
    
    public int compare(ScoreDoc aDoc, ScoreDoc bDoc) {
      
//      if (this.distances == null) {
//          distances = distanceFilter.getDistances();
//      }
      double a = distanceFilter.getDistance(aDoc.doc);
      double b = distanceFilter.getDistance(bDoc.doc);
      if (a > b) return 1;
      if (a < b )return -1;
      
      return 0;
    }

    public int sortType() {
      return SortField.DOUBLE;
    }

    public Comparable sortValue(ScoreDoc iDoc) {
      return distanceFilter.getDistance(iDoc.doc);
    }
    
    public void cleanUp() {
      distanceFilter = null;
    }
  }
}
