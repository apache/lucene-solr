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
package org.apache.solr.client.solrj.io.stream.expr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Explanation containing details about a stream expression
 */
public class StreamExplanation extends Explanation {
  
  private List<Explanation> children;
  
  public StreamExplanation(String expressionNodeId){
    super(expressionNodeId);
  }
  
  public List<Explanation> getChildren(){
    return children;
  }
  public void setChildren(List<Explanation> children){
    this.children = children;
  }
  public StreamExplanation withChildren(List<Explanation> children){
    setChildren(children);
    return this;
  }
  public StreamExplanation withChildren(Explanation[] children){
    for(Explanation child : children){
      addChild(child);
    }
    return this;
  }
  public void addChild(Explanation child){
    if(null == children){
      children = new ArrayList<Explanation>(1);
    }
    children.add(child);
  }
  
  public Map<String,Object> toMap(Map<String,Object> map){
    map = super.toMap(map);
    
    if(null != children && 0 != children.size()){
      List<Map<String,Object>> childrenMaps = new ArrayList<Map<String,Object>>();
      for(Explanation child : children){
        childrenMaps.add(child.toMap(new LinkedHashMap<>()));
      }
      map.put("children", childrenMaps);
    }
    
    return map;
  }
}
