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

package org.apache.solr.common.params;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;

/**  SolrParams hold request parameters.
 *
 *
 */
public abstract class SolrParams implements Serializable {

  /** returns the String value of a param, or null if not set */
  public abstract String get(String param);

  /** returns an array of the String values of a param, or null if none */
  public abstract String[] getParams(String param);

  /** returns an Iterator over the parameter names */
  public abstract Iterator<String> getParameterNamesIterator();

  /** returns the value of the param, or def if not set */
  public String get(String param, String def) {
    String val = get(param);
    return val==null ? def : val;
  }
  
  /** returns a RequiredSolrParams wrapping this */
  public RequiredSolrParams required()
  {
    // TODO? should we want to stash a reference?
    return new RequiredSolrParams(this);
  }
  
  protected String fpname(String field, String param) {
    return "f."+field+'.'+param;
  }

  /** returns the String value of the field parameter, "f.field.param", or
   *  the value for "param" if that is not set.
   */
  public String getFieldParam(String field, String param) {
    String val = get(fpname(field,param));
    return val!=null ? val : get(param);
  }

  /** returns the String value of the field parameter, "f.field.param", or
   *  the value for "param" if that is not set.  If that is not set, def
   */
  public String getFieldParam(String field, String param, String def) {
    String val = get(fpname(field,param));
    return val!=null ? val : get(param, def);
  }
  
  /** returns the String values of the field parameter, "f.field.param", or
   *  the values for "param" if that is not set.
   */
  public String[] getFieldParams(String field, String param) {
    String[] val = getParams(fpname(field,param));
    return val!=null ? val : getParams(param);
  }

  /** Returns the Boolean value of the param, or null if not set */
  public Boolean getBool(String param) {
    String val = get(param);
    return val==null ? null : StrUtils.parseBool(val);
  }

  /** Returns the boolean value of the param, or def if not set */
  public boolean getBool(String param, boolean def) {
    String val = get(param);
    return val==null ? def : StrUtils.parseBool(val);
  }
  
  /** Returns the Boolean value of the field param, 
      or the value for param, or null if neither is set. */
  public Boolean getFieldBool(String field, String param) {
    String val = getFieldParam(field, param);
    return val==null ? null : StrUtils.parseBool(val);
  }
  
  /** Returns the boolean value of the field param, 
  or the value for param, or def if neither is set. */
  public boolean getFieldBool(String field, String param, boolean def) {
    String val = getFieldParam(field, param);
    return val==null ? def : StrUtils.parseBool(val);
  }

  /** Returns the Integer value of the param, or null if not set */
  public Integer getInt(String param) {
    String val = get(param);
    try {
      return val==null ? null : Integer.valueOf(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the Long value of the param, or null if not set */
  public Long getLong(String param, Long def) {
    String val = get(param);
    try {
      return val== null ? def : Long.parseLong(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the int value of the param, or def if not set */
  public int getInt(String param, int def) {
    String val = get(param);
    try {
      return val==null ? def : Integer.parseInt(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }
  
  /**
   * @return The int value of the field param, or the value for param 
   * or <code>null</code> if neither is set. 
   **/
  public Integer getFieldInt(String field, String param) {
    String val = getFieldParam(field, param);
    try {
      return val==null ? null : Integer.valueOf(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }
  
  /** Returns the int value of the field param, 
  or the value for param, or def if neither is set. */
  public int getFieldInt(String field, String param, int def) {
    String val = getFieldParam(field, param);
    try {
      return val==null ? def : Integer.parseInt(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }


  /** Returns the Float value of the param, or null if not set */
  public Float getFloat(String param) {
    String val = get(param);
    try {
      return val==null ? null : Float.valueOf(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the float value of the param, or def if not set */
  public float getFloat(String param, float def) {
    String val = get(param);
    try {
      return val==null ? def : Float.parseFloat(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the Float value of the param, or null if not set */
  public Double getDouble(String param) {
    String val = get(param);
    try {
      return val==null ? null : Double.valueOf(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the float value of the param, or def if not set */
  public double getDouble(String param, double def) {
    String val = get(param);
    try {
      return val==null ? def : Double.parseDouble(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }


  /** Returns the float value of the field param. */
  public Float getFieldFloat(String field, String param) {
    String val = getFieldParam(field, param);
    try {
      return val==null ? null : Float.valueOf(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the float value of the field param,
  or the value for param, or def if neither is set. */
  public float getFieldFloat(String field, String param, float def) {
    String val = getFieldParam(field, param);
    try {
      return val==null ? def : Float.parseFloat(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the float value of the field param. */
  public Double getFieldDouble(String field, String param) {
    String val = getFieldParam(field, param);
    try {
      return val==null ? null : Double.valueOf(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  /** Returns the float value of the field param,
  or the value for param, or def if neither is set. */
  public double getFieldDouble(String field, String param, double def) {
    String val = getFieldParam(field, param);
    try {
      return val==null ? def : Double.parseDouble(val);
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, ex.getMessage(), ex );
    }
  }

  @SuppressWarnings({"deprecation"})
  public static SolrParams wrapDefaults(SolrParams params, SolrParams defaults) {
    if (params == null)
      return defaults;
    if (defaults == null)
      return params;
    return new DefaultSolrParams(params,defaults);
  }

  @SuppressWarnings({"deprecation"})
  public static SolrParams wrapAppended(SolrParams params, SolrParams defaults) {
    if (params == null)
      return defaults;
    if (defaults == null)
      return params;
    return new AppendedSolrParams(params,defaults);
  }

  /** Create a Map&lt;String,String&gt; from a NamedList given no keys are repeated */
  public static Map<String,String> toMap(NamedList params) {
    HashMap<String,String> map = new HashMap<>();
    for (int i=0; i<params.size(); i++) {
      map.put(params.getName(i), params.getVal(i).toString());
    }
    return map;
  }

  /** Create a Map&lt;String,String[]&gt; from a NamedList */
  public static Map<String,String[]> toMultiMap(NamedList params) {
    HashMap<String,String[]> map = new HashMap<>();
    for (int i=0; i<params.size(); i++) {
      String name = params.getName(i);
      String val = params.getVal(i).toString();
      MultiMapSolrParams.addParam(name,val,map);
    }
    return map;
  }

  /** Create SolrParams from NamedList. */
  public static SolrParams toSolrParams(NamedList params) {
    // if no keys are repeated use the faster MapSolrParams
    HashMap<String,String> map = new HashMap<>();
    for (int i=0; i<params.size(); i++) {
      String prev = map.put(params.getName(i), params.getVal(i).toString());
      if (prev!=null) return new MultiMapSolrParams(toMultiMap(params));
    }
    return new MapSolrParams(map);
  }
  
  /** Create filtered SolrParams. */
  public SolrParams toFilteredSolrParams(List<String> names) {
    NamedList<String> nl = new NamedList<>();
    for (Iterator<String> it = getParameterNamesIterator(); it.hasNext();) {
      final String name = it.next();
      if (names.contains(name)) {
        final String[] values = getParams(name);
        for (String value : values) {
          nl.add(name, value);
        }
      }
    }
    return toSolrParams(nl);
  }
  
  /** Convert this to a NamedList */
  public NamedList<Object> toNamedList() {
    final SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
    
    for(Iterator<String> it=getParameterNamesIterator(); it.hasNext(); ) {
      final String name = it.next();
      final String [] values = getParams(name);
      if(values.length==1) {
        result.add(name,values[0]);
      } else {
        // currently no reason not to use the same array
        result.add(name,values);
      }
    }
    return result;
  }
}
