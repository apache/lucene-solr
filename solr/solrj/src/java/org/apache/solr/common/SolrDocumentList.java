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
package org.apache.solr.common;

import java.util.ArrayList;


/**
 * Represent a list of SolrDocuments returned from a search.  This includes
 * position and offset information.
 * 
 *
 * @since solr 1.3
 */
public class SolrDocumentList extends ArrayList<SolrDocument>
{ 
  private long numFound = 0;
  private long start = 0;
  private Float maxScore = null;
  private Boolean numFoundExact = true;
  
  public Boolean getNumFoundExact() {
    return numFoundExact;
  }

  public void setNumFoundExact(Boolean numFoundExact) {
    this.numFoundExact = numFoundExact;
  }

  public Float getMaxScore() {
    return maxScore;
  }
  
  public void setMaxScore(Float maxScore) {
    this.maxScore = maxScore;
  }
  
  public long getNumFound() {
    return numFound;
  }
  
  public void setNumFound(long numFound) {
    this.numFound = numFound;
  }
  
  public long getStart() {
    return start;
  }
  
  public void setStart(long start) {
    this.start = start;
  }

  @Override
  public String toString() {
    return "{numFound="+numFound
            +",numFoundExact="+String.valueOf(numFoundExact)
            +",start="+start
            + (maxScore!=null ? ",maxScore="+maxScore : "")
            +",docs="+super.toString()
            +"}";
  }
}
