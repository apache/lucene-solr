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

import java.util.Objects;

/**
 * Basic string stream expression
 */
public class StreamExpressionValue implements StreamExpressionParameter {
  
  private String value;
  
  public StreamExpressionValue(String value){
    this.value = value;
  }
  
  public String getValue(){
    return this.value;
  }
  
  public void setValue(String value){
    this.value = value;
  }
  
  public StreamExpressionValue withValue(String value){
    this.value = value;
    return this;
  }
  
  @Override
  public String toString(){
    return this.value;
  }
  
  @Override
  public boolean equals(Object other){
    if(other.getClass() != StreamExpressionValue.class){
      return false;
    }
    
    StreamExpressionValue check = (StreamExpressionValue)other;
    
    if(null == this.value && null == check.value){
      return true;
    }
    if(null == this.value || null == check.value){
      return false;
    }
    
    return this.value.equals(((StreamExpressionValue)other).value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
