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
import java.util.List;
import java.util.Objects;

/**
 * Expression containing a function and set of parameters
 */
public class StreamExpression implements StreamExpressionParameter {
  private String functionName;
  private List<StreamExpressionParameter> parameters;
  
  public StreamExpression(String functionName){
    this.functionName = functionName;
    parameters = new ArrayList<StreamExpressionParameter>();
  }
  
  public String getFunctionName(){
    return this.functionName;
  }
  public void setFunctionName(String functionName){
    if(null == functionName){
      throw new IllegalArgumentException("Null functionName is not allowed.");
    }
    
    this.functionName = functionName;
  }
  public StreamExpression withFunctionName(String functionName){
    setFunctionName(functionName);
    return this;
  }  
  
  public void addParameter(StreamExpressionParameter parameter){
    this.parameters.add(parameter);
  }
  public void addParameter(String parameter){
    addParameter(new StreamExpressionValue(parameter));
  }

  public StreamExpression withParameter(StreamExpressionParameter parameter){
    this.parameters.add(parameter);
    return this;
  }
  public StreamExpression withParameter(String parameter){
    return withParameter(new StreamExpressionValue(parameter));
  }
  
  public List<StreamExpressionParameter> getParameters(){
    return this.parameters;
  }
  public void setParameters(List<StreamExpressionParameter> parameters){
    if(null == parameters){
      throw new IllegalArgumentException("Null parameter list is not allowed.");
    }
    
    this.parameters = parameters;
  }
  public StreamExpression withParameters(List<StreamExpressionParameter> parameters){
    setParameters(parameters);
    return this;
  }
  
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder(this.functionName);
    
    sb.append("(");
    for(int idx = 0; idx < parameters.size(); ++idx){
      if(0 != idx){ sb.append(","); }
      sb.append(parameters.get(idx));
    }
    sb.append(")");
    
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object other){
    if(other.getClass() != StreamExpression.class){
      return false;
    }
    
    StreamExpression check = (StreamExpression)other;
    
    if(null == this.functionName && null != check.functionName){
      return false;
    }
    if(null != this.functionName && null == check.functionName){
      return false;
    }
    
    if(null != this.functionName && null != check.functionName && !this.functionName.equals(check.functionName)){
      return false;
    }
    
    if(this.parameters.size() != check.parameters.size()){
      return false;
    }
    
    for(int idx = 0; idx < this.parameters.size(); ++idx){
      StreamExpressionParameter left = this.parameters.get(idx);
      StreamExpressionParameter right = check.parameters.get(idx);
      if(!left.equals(right)){
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionName);
  }
}
