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

import org.apache.solr.common.MapSerializable;


/**
 * Explanation containing details about a expression
 */
public class Explanation implements MapSerializable {
  
  private String expressionNodeId;
  private String expressionType;
  private String functionName;
  private String implementingClass;
  private String expression;
  private String note;
  private List<Explanation> helpers;
  
  public Explanation(String expressionNodeId){
    this.expressionNodeId = expressionNodeId;
  }
  
  public String getExpressionNodeId(){
    return expressionNodeId;
  }

  public String getExpressionType(){
    return expressionType;
  }
  public void setExpressionType(String expressionType){
    this.expressionType = expressionType;
  }
  public Explanation withExpressionType(String expressionType){
    setExpressionType(expressionType);
    return this;
  }

  public String getFunctionName(){
    return functionName;
  }
  public void setFunctionName(String functionName){
    this.functionName = functionName;
  }
  public Explanation withFunctionName(String functionName){
    setFunctionName(functionName);
    return this;
  }

  public String getImplementingClass(){
    return implementingClass;
  }
  public void setImplementingClass(String implementingClass){
    this.implementingClass = implementingClass;
  }
  public Explanation withImplementingClass(String implementingClass){
    setImplementingClass(implementingClass);
    return this;
  }
  
  public String getExpression(){
    return expression;
  }
  public void setExpression(String expression){
    this.expression = expression;
  }
  public Explanation withExpression(String expression){
    setExpression(expression);
    return this;
  }
  
  public String getNote(){
    return note;
  }
  public void setNote(String note){
    this.note = note;
  }
  public Explanation withNote(String note){
    setNote(note);
    return this;
  }
  
  public List<Explanation> getHelpers(){
    return helpers;
  }
  public void setHelpers(List<Explanation> helpers){
    this.helpers = helpers;
  }
  public Explanation withHelpers(List<Explanation> helpers){
    setHelpers(helpers);
    return this;
  }
  public Explanation withHelpers(Explanation[] helpers){
    for(Explanation helper : helpers){
      addHelper(helper);
    }
    return this;
  }
  public Explanation withHelper(Explanation helper){
    addHelper(helper);
    return this;
  }
  public void addHelper(Explanation helper){
    if(null == helpers){
      helpers = new ArrayList<Explanation>(1);
    }
    helpers.add(helper);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Map toMap(Map<String, Object> map) {
    if(null != expressionNodeId){ map.put("expressionNodeId",expressionNodeId); }
    if(null != expressionType){ map.put("expressionType",expressionType); }
    if(null != functionName){ map.put("functionName",functionName); }
    if(null != implementingClass){ map.put("implementingClass",implementingClass); }
    if(null != expression){ map.put("expression",expression); }
    if(null != note){ map.put("note",note); }

    if(null != helpers && 0 != helpers.size()){
      List<Map<String,Object>> helperMaps = new ArrayList<>();
      for(Explanation helper : helpers){
        helperMaps.add(helper.toMap(new LinkedHashMap<>()));
      }
      map.put("helpers", helperMaps);
    }

    return map;
  }
  
  public static interface ExpressionType{
    public static final String GRAPH_SOURCE = "graph-source";
    public static final String MACHINE_LEARNING_MODEL = "ml-model";
    public static final String STREAM_SOURCE = "stream-source";
    public static final String STREAM_DECORATOR = "stream-decorator";
    public static final String DATASTORE = "datastore";
    public static final String METRIC = "metric";
    public static final String OPERATION = "operation";
    public static final String EVALUATOR = "evaluator";
    public static final String EQUALITOR = "equalitor";
    public static final String SORTER = "sorter";
  }
}
