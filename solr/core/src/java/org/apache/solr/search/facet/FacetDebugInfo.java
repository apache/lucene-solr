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
package org.apache.solr.search.facet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.SimpleOrderedMap;

public class FacetDebugInfo {
  String processor;
  long elapse = -1;
  String filter;
  Map<String, Object> info;  // additional information
  final List<FacetDebugInfo> children;
  
  Map<String, Object> reqDescription;
  
  public FacetDebugInfo() {
    children = new ArrayList<FacetDebugInfo>();
    info = new LinkedHashMap<String, Object>();
  }
  
  public void addChild(FacetDebugInfo child) {
    children.add(child);
  }
  
  public void setProcessor(String processor) {
    this.processor = processor;
  }

  public void setElapse(long elapse) {
    this.elapse = elapse;
  }

  public void setReqDescription(Map<String, Object> reqDescription) {
    this.reqDescription = reqDescription;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public void putInfoItem(String key, Object value) {
    info.put(key, value);
  }
  
  public Map<String, Object> getInfo() {
    return info;
  }
  
  public SimpleOrderedMap<Object> getFacetDebugInfo() {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
    
    if (filter != null) info.add("filter", filter);
    if (processor != null) info.add("processor", processor);
    if (elapse != -1) info.add("elapse", elapse);
    if (reqDescription != null) {
      info.addAll(reqDescription);
    } 
    info.addAll(this.info);
    
    if (children != null && children.size() > 0) {
      List<Object> subfacet = new ArrayList<Object>();
      info.add("sub-facet", subfacet);
      for (FacetDebugInfo child : children) {
        subfacet.add(child.getFacetDebugInfo());
      }
    }     
    return info;
  } 

  @Override
  public String toString() {
    return "facet debug info: processor " + processor + " elapse " + elapse + "ms";
  }
}
