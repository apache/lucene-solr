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

import org.apache.solr.common.SolrException;

import java.util.Iterator;

/**
 * This is a simple wrapper to SolrParams that will throw a 400
 * exception if you ask for a parameter that does not exist.  Fields
 * specified with
 * 
 * In short, any value you for from a <code>RequiredSolrParams</code> 
 * will return a valid non-null value or throw a 400 exception.  
 * (If you pass in <code>null</code> as the default value, you can 
 * get a null return value)
 * 
 *
 * @since solr 1.2
 */
public class RequiredSolrParams extends SolrParams {
  protected final SolrParams params;
  
  public RequiredSolrParams(SolrParams params) {
    this.params = params;
  }

  /** get the param from params, fail if not found **/
  @Override
  public String get(String param) {
    String val = params.get(param);
    if( val == null )  {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: "+param );
    }
    return val;
  }
  
  @Override
  public String getFieldParam(final String field, final String param) {
    final String fpname = fpname(field,param);
    String val = params.get(fpname);
    if (null == val) {
      // don't call this.get, we want a specified exception message
      val = params.get(param);
      if (null == val)  {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                                 "Missing required parameter: "+fpname+
                                 " (or default: "+param+")" );
      }
    }
    return val;
  }

  @Override
  public String[] getFieldParams(final String field, final String param) {
    final String fpname = fpname(field,param);
    String[] val = params.getParams(fpname);
    if (null == val) {
      // don't call this.getParams, we want a specified exception message
      val = params.getParams(param);
      if (null == val)  {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                                 "Missing required parameter: "+fpname+
                                 " (or default: "+param+")" );
      }
    }
    return val;
  }

  
  @Override
  public String[] getParams(String param) {
    String[] vals = params.getParams(param);
    if( vals == null || vals.length == 0 ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: "+param );
    }
    return vals;
  }
  
  /** returns an Iterator over the parameter names */
  @Override
  public Iterator<String> getParameterNamesIterator() {
    return params.getParameterNamesIterator();
  }

  @Override
  public String toString() {
    return "{required("+params+")}";  
  }    

  //----------------------------------------------------------
  // Functions with a default value - pass directly to the
  // wrapped SolrParams (they won't return null - unless it's the default)
  //----------------------------------------------------------

  @Override
  public String get(String param, String def) {
    return params.get(param, def);
  }

  @Override
  public int getInt(String param, int def) {
    return params.getInt(param, def);
  }

  @Override
  public float getFloat(String param, float def) {
    return params.getFloat(param, def);
  }
  
  @Override
  public boolean getBool(String param, boolean def) {
    return params.getBool(param, def);
  }

  @Override
  public int getFieldInt(String field, String param, int def) {
    return params.getFieldInt(field, param, def);
  }
  
  @Override
  public boolean getFieldBool(String field, String param, boolean def) {
    return params.getFieldBool(field, param, def);
  }

  @Override
  public float getFieldFloat(String field, String param, float def) {
    return params.getFieldFloat(field, param, def);
  }

  @Override
  public String getFieldParam(String field, String param, String def) {
    return params.getFieldParam(field, param, def);
  }

  public void check(String... params){
    for (String param : params) get(param);
  }
}
