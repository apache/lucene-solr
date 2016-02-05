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
package org.apache.solr.client.solrj.io.stream.metrics;

import java.io.Serializable;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;

public abstract class Metric implements Serializable, Expressible {
  
  private static final long serialVersionUID = 1L;
  private String functionName;
  private String identifier;
  
//  @Override
  public String getFunctionName(){
    return functionName;
  }
  
//  @Override
  public void setFunctionName(String functionName){
    this.functionName = functionName;
  }
  
  public String getIdentifier(){
    return identifier;
  }
  public void setIdentifier(String identifier){
    this.identifier = identifier;
  }
  public void setIdentifier(String ... identifierParts){
    StringBuilder sb = new StringBuilder();
    for(String part : identifierParts){
      sb.append(part);
    }
    this.identifier = sb.toString();
  }
  
  public abstract double getValue();
  public abstract void update(Tuple tuple);
  public abstract Metric newInstance();
}