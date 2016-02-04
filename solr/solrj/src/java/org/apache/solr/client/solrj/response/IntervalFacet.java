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

import java.util.List;

/**
 * Objects of this class will contain the result of all the intervals defined
 * for a specific field. 
 */
public class IntervalFacet {
 
  /**
   * The field for which interval facets where calculated
   */
  private final String field;

  /**
   * The list of interval facets calculated for {@link #field}
   */
  private final List<Count> intervals;
  
  IntervalFacet(String field, List<Count> values) {
    this.field = field;
    this.intervals = values;
  }
  
  /**
   * @return The field for which interval facets where calculated
   */
  public String getField() {
    return field;
  }

  /**
   * @return The list of interval facets calculated for {@link #field}
   */
  public List<Count> getIntervals() {
    return intervals;
  }
  
  /**
   * Holds counts for facet intervals defined in a field
   */
  public static class Count {
    /**
     * The key of this interval. This is the original 
     * interval string or the value of the "key" local
     * param
     */
    private final String key;
    /**
     * The count of this interval
     */
    private final int count;
    
    Count(String key, int count) {
      super();
      this.key = key;
      this.count = count;
    }
    
    public String getKey() {
      return key;
    }

    public int getCount() {
      return count;
    }
  }
}
