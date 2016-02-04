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
package org.apache.solr.analytics.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.analytics.request.FieldFacetRequest.FacetSortSpecification;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.IndexSchema;

/**
 * Parses the SolrParams to create a list of analytics requests.
 */
public class AnalyticsRequestFactory implements AnalyticsParams {

  public static final Pattern statPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+EXPRESSION+")\\.([^\\.]+)$", Pattern.CASE_INSENSITIVE);
  public static final Pattern hiddenStatPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+HIDDEN_EXPRESSION+")\\.([^\\.]+)$", Pattern.CASE_INSENSITIVE);
  public static final Pattern fieldFacetPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+FIELD_FACET+")$", Pattern.CASE_INSENSITIVE);
  public static final Pattern fieldFacetParamPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+FIELD_FACET+")\\.([^\\.]+)\\.("+LIMIT+"|"+OFFSET+"|"+HIDDEN+"|"+SHOW_MISSING+"|"+SORT_STATISTIC+"|"+SORT_DIRECTION+")$", Pattern.CASE_INSENSITIVE);
  public static final Pattern rangeFacetPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+RANGE_FACET+")$", Pattern.CASE_INSENSITIVE);
  public static final Pattern rangeFacetParamPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+RANGE_FACET+")\\.([^\\.]+)\\.("+START+"|"+END+"|"+GAP+"|"+HARDEND+"|"+INCLUDE_BOUNDARY+"|"+OTHER_RANGE+")$", Pattern.CASE_INSENSITIVE);
  public static final Pattern queryFacetPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+QUERY_FACET+")$", Pattern.CASE_INSENSITIVE);
  public static final Pattern queryFacetParamPattern = Pattern.compile("^o(?:lap)?\\.([^\\.]+)\\.(?:"+QUERY_FACET+")\\.([^\\.]+)\\.("+QUERY+"|"+DEPENDENCY+")$", Pattern.CASE_INSENSITIVE);
  
  public static List<AnalyticsRequest> parse(IndexSchema schema, SolrParams params) {
    Map<String, AnalyticsRequest> requestMap = new HashMap<>();
    Map<String, Map<String,FieldFacetRequest>> fieldFacetMap = new HashMap<>();
    Map<String, Set<String>> fieldFacetSet = new HashMap<>();
    Map<String, Map<String,RangeFacetRequest>> rangeFacetMap = new HashMap<>();
    Map<String, Set<String>> rangeFacetSet = new HashMap<>();
    Map<String, Map<String,QueryFacetRequest>> queryFacetMap = new HashMap<>();
    Map<String, Set<String>> queryFacetSet = new HashMap<>();
    List<AnalyticsRequest> requestList = new ArrayList<>();
    
    Iterator<String> paramsIterator = params.getParameterNamesIterator();
    while (paramsIterator.hasNext()) {
      String param = paramsIterator.next();
      CharSequence paramSequence = param.subSequence(0, param.length());
      
      // Check if stat
      Matcher m = statPattern.matcher(paramSequence);
      if (m.matches()) {
        makeExpression(requestMap,m.group(1),m.group(2),params.get(param));
      } else {
        // Check if hidden stat
        m = hiddenStatPattern.matcher(paramSequence);
        if (m.matches()) {
          makeHiddenExpression(requestMap,m.group(1),m.group(2),params.get(param));
        } else {
          // Check if field facet
          m = fieldFacetPattern.matcher(paramSequence);
          if (m.matches()) {
            makeFieldFacet(schema,fieldFacetMap,fieldFacetSet,m.group(1),params.getParams(param));
          } else {
            // Check if field facet parameter
            m = fieldFacetParamPattern.matcher(paramSequence);
            if (m.matches()) {
              setFieldFacetParam(schema,fieldFacetMap,m.group(1),m.group(2),m.group(3),params.getParams(param));
            } else {
              // Check if range facet
              m = rangeFacetPattern.matcher(paramSequence);
              if (m.matches()) {
                makeRangeFacet(schema,rangeFacetSet,m.group(1),params.getParams(param));
              }  else {
                // Check if range facet parameter
                m = rangeFacetParamPattern.matcher(paramSequence);
                if (m.matches()) {
                  setRangeFacetParam(schema,rangeFacetMap,m.group(1),m.group(2),m.group(3),params.getParams(param));
                }  else {
                  // Check if query facet
                  m = queryFacetPattern.matcher(paramSequence);
                  if (m.matches()) {
                    makeQueryFacet(schema,queryFacetSet,m.group(1),params.getParams(param));
                  }  else {
                    // Check if query
                    m = queryFacetParamPattern.matcher(paramSequence);
                    if (m.matches()) {
                      setQueryFacetParam(schema,queryFacetMap,m.group(1),m.group(2),m.group(3),params.getParams(param));
                    } 
                  }
                }
              }
            }
          }
        }
      }
    }
    for (String reqName : requestMap.keySet()) {
      AnalyticsRequest ar = requestMap.get(reqName);
      List<FieldFacetRequest> ffrs = new ArrayList<>();
      if (fieldFacetSet.get(reqName)!=null) {
        for (String field : fieldFacetSet.get(reqName)) {
          ffrs.add(fieldFacetMap.get(reqName).get(field));
        }
      }
      ar.setFieldFacets(ffrs);
      
      List<RangeFacetRequest> rfrs = new ArrayList<>();
      if (rangeFacetSet.get(reqName)!=null) {
        for (String field : rangeFacetSet.get(reqName)) {
          RangeFacetRequest rfr = rangeFacetMap.get(reqName).get(field);
          if (rfr != null) {
            rfrs.add(rfr);
          }
        }
      }
      ar.setRangeFacets(rfrs);
      
      List<QueryFacetRequest> qfrs = new ArrayList<>();
      if (queryFacetSet.get(reqName)!=null) {
        for (String name : queryFacetSet.get(reqName)) {
          QueryFacetRequest qfr = queryFacetMap.get(reqName).get(name);
          if (qfr != null) {
            addQueryFacet(qfrs,qfr);
          }
        }
      }
      for (QueryFacetRequest qfr : qfrs) {
        if (qfr.getDependencies().size()>0) {
          throw new SolrException(ErrorCode.BAD_REQUEST,"The query facet dependencies "+qfr.getDependencies().toString()+" either do not exist or are defined in a dependency looop.");
        }
      }
      ar.setQueryFacets(qfrs);
      requestList.add(ar);
    }
    return requestList; 
  }

  private static void makeFieldFacet(IndexSchema schema, Map<String, Map<String, FieldFacetRequest>> fieldFacetMap, Map<String, Set<String>> fieldFacetSet, String requestName, String[] fields) {
    Map<String, FieldFacetRequest> facetMap = fieldFacetMap.get(requestName);
    if (facetMap == null) {
      facetMap = new HashMap<>();
      fieldFacetMap.put(requestName, facetMap);
    }
    Set<String> set = fieldFacetSet.get(requestName);
    if (set == null) {
      set = new HashSet<>();
      fieldFacetSet.put(requestName, set);
    }
    for (String field : fields) {
      if (facetMap.get(field) == null) {
        facetMap.put(field,new FieldFacetRequest(schema.getField(field)));
      }
      set.add(field);
    }
  }

  private static void setFieldFacetParam(IndexSchema schema, Map<String, Map<String, FieldFacetRequest>> fieldFacetMap, String requestName, String field, String paramType, String[] params) {
    Map<String, FieldFacetRequest> facetMap = fieldFacetMap.get(requestName);
    if (facetMap == null) {
      facetMap = new HashMap<>();
      fieldFacetMap.put(requestName, facetMap);
    }
    FieldFacetRequest fr = facetMap.get(field);
    if (fr == null) {
      fr = new FieldFacetRequest(schema.getField(field));
      facetMap.put(field,fr);
    }
    if (paramType.equals("limit")||paramType.equals("l")) {
      fr.setLimit(Integer.parseInt(params[0]));
    } else if (paramType.equals("offset")||paramType.equals("off")) {
      fr.setOffset(Integer.parseInt(params[0]));
    } else if (paramType.equals("hidden")||paramType.equals("h")) {
      fr.setHidden(Boolean.parseBoolean(params[0]));
    } else if (paramType.equals("showmissing")||paramType.equals("sm")) {
      fr.showMissing(Boolean.parseBoolean(params[0]));
    } else if (paramType.equals("sortstatistic")||paramType.equals("sortstat")||paramType.equals("ss")) {
      fr.setSort(new FacetSortSpecification(params[0],fr.getDirection()));
    } else if (paramType.equals("sortdirection")||paramType.equals("sd")) {
      fr.setDirection(params[0]);
    } 
  }

  private static void makeRangeFacet(IndexSchema schema, Map<String, Set<String>> rangeFacetSet, String requestName, String[] fields) {
    Set<String> set = rangeFacetSet.get(requestName);
    if (set == null) {
      set = new HashSet<>();
      rangeFacetSet.put(requestName, set);
    }
    for (String field : fields) {
      set.add(field);
    }
  }

  private static void setRangeFacetParam(IndexSchema schema, Map<String, Map<String, RangeFacetRequest>> rangeFacetMap, String requestName, String field, String paramType, String[] params) {
    Map<String, RangeFacetRequest> facetMap = rangeFacetMap.get(requestName);
    if (facetMap == null) {
      facetMap = new HashMap<>();
      rangeFacetMap.put(requestName, facetMap);
    }
    RangeFacetRequest rr = facetMap.get(field);
    if (rr == null) {
      rr = new RangeFacetRequest(schema.getField(field));
      facetMap.put(field,rr);
    }
    if (paramType.equals("start")||paramType.equals("st")) {
      rr.setStart(params[0]);
    } else if (paramType.equals("end")||paramType.equals("e")) {
      rr.setEnd(params[0]);
    } else if (paramType.equals("gap")||paramType.equals("g")) {
      rr.setGaps(params[0].split(","));
    } else if (paramType.equals("hardend")||paramType.equals("he")) {
      rr.setHardEnd(Boolean.parseBoolean(params[0]));
    } else if (paramType.equals("includebound")||paramType.equals("ib")) {
      for (String param : params) {
        rr.addInclude(FacetRangeInclude.get(param));
      }
    } else if (paramType.equals("otherrange")||paramType.equals("or")) {
      for (String param : params) {
        rr.addOther(FacetRangeOther.get(param));
      }
    } 
  }

  private static void makeQueryFacet(IndexSchema schema,Map<String, Set<String>> queryFacetSet, String requestName, String[] names) {
    Set<String> set = queryFacetSet.get(requestName);
    if (set == null) {
      set = new HashSet<>();
      queryFacetSet.put(requestName, set);
    }
    for (String name : names) {
      set.add(name);
    }
  }

  private static void setQueryFacetParam(IndexSchema schema, Map<String, Map<String, QueryFacetRequest>> queryFacetMap, String requestName, String name, String paramType, String[] params) {
    Map<String, QueryFacetRequest> facetMap = queryFacetMap.get(requestName);
    if (facetMap == null) {
      facetMap = new HashMap<>();
      queryFacetMap.put(requestName, facetMap);
    }
    QueryFacetRequest qr = facetMap.get(name);
    if (qr == null) {
      qr = new QueryFacetRequest(name);
      facetMap.put(name,qr);
    }
    if (paramType.equals("query")||paramType.equals("q")) {
      for (String query : params) {
        qr.addQuery(query);
      }
    } else if (paramType.equals("dependency")||paramType.equals("d")) {
      for (String depend : params) {
        qr.addDependency(depend);
      }
    }
  }

  private static void makeHiddenExpression(Map<String, AnalyticsRequest> requestMap, String requestName, String expressionName, String expression) {
    AnalyticsRequest req = requestMap.get(requestName);
    if (req == null) {
      req = new AnalyticsRequest(requestName);
      requestMap.put(requestName, req);
    }
    req.addHiddenExpression(new ExpressionRequest(expressionName,expression));
  }

  private static void makeExpression(Map<String, AnalyticsRequest> requestMap, String requestName, String expressionName, String expression) {
    AnalyticsRequest req = requestMap.get(requestName);
    if (req == null) {
      req = new AnalyticsRequest(requestName);
      requestMap.put(requestName, req);
    }
    req.addExpression(new ExpressionRequest(expressionName,expression));
  }
  
  private static void addQueryFacet(List<QueryFacetRequest> currentList, QueryFacetRequest queryFacet) {
    Set<String> depends = queryFacet.getDependencies();
    int place = 0;
    for (QueryFacetRequest qfr : currentList) {
      if (qfr.getDependencies().remove(queryFacet.getName())) {
        break;
      }
      place++;
      depends.remove(qfr.getName());
    }
    currentList.add(place,queryFacet);
    for (int count = place+1; count < currentList.size(); count++) {
      currentList.get(count).getDependencies().remove(queryFacet.getName());
    }
  }
}
