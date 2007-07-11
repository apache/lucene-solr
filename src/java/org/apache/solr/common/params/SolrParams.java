/**
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**  SolrParams hold request parameters.
 *
 * @version $Id$
 */
public abstract class SolrParams {
  /** the query type - which query handler should handle the request */
  public static final String QT ="qt";
  /** the response writer type - the format of the response */
  public static final String WT ="wt";
  /** query string */
  public static final String Q ="q";
  /** sort order */
  public static final String SORT ="sort";
  /** Lucene query string(s) for filtering the results without affecting scoring */
  public static final String FQ ="fq";
  /** zero based offset of matching documents to retrieve */
  public static final String START ="start";
  /** number of documents to return starting at "start" */
  public static final String ROWS ="rows";
  /** stylesheet to apply to XML results */
  public static final String XSL ="xsl";
  /** stylesheet to apply to XML results */
  public static final String VERSION ="version";
  /** query and init param for field list */
  public static final String FL = "fl";
  /** default query field */
  public static final String DF = "df";
  /** whether to include debug data */
  public static final String DEBUG_QUERY = "debugQuery";
  /** another query to explain against */
  public static final String EXPLAIN_OTHER = "explainOther";

  /**
   * Should facet counts be calculated?
   */
  public static final String FACET = "facet";
  
  /**
   * Any lucene formated queries the user would like to use for
   * Facet Contraint Counts (multi-value)
   */
  public static final String FACET_QUERY = "facet.query";
  /**
   * Any field whose terms the user wants to enumerate over for
   * Facet Contraint Counts (multi-value)
   */
  public static final String FACET_FIELD = "facet.field";

  /**
   * The offset into the list of facets.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_OFFSET = "facet.offset";

  /**
   * Numeric option indicating the maximum number of facet field counts
   * be included in the response for each field - in descending order of count.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_LIMIT = "facet.limit";

  /**
   * Numeric option indicating the minimum number of hits before a facet should
   * be included in the response.  Can be overriden on a per field basis.
   */
  public static final String FACET_MINCOUNT = "facet.mincount";

  /**
   * Boolean option indicating whether facet field counts of "0" should 
   * be included in the response.  Can be overriden on a per field basis.
   */
  public static final String FACET_ZEROS = "facet.zeros";

  /**
   * Boolean option indicating whether the response should include a 
   * facet field count for all records which have no value for the 
   * facet field. Can be overriden on a per field basis.
   */
  public static final String FACET_MISSING = "facet.missing";

  /**
   * Boolean option: true causes facets to be sorted
   * by the count, false results in natural index order.
   */
  public static final String FACET_SORT = "facet.sort";

  /**
   * Only return constraints of a facet field with the given prefix.
   */
  public static final String FACET_PREFIX = "facet.prefix";

 /**
   * When faceting by enumerating the terms in a field,
   * only use the filterCache for terms with a df >= to this parameter.
   */
  public static final String FACET_ENUM_CACHE_MINDF = "facet.enum.cache.minDf";


  /** If the content stream should come from a URL (using URLConnection) */
  public static final String STREAM_URL = "stream.url";

  /** If the content stream should come from a File (using FileReader) */
  public static final String STREAM_FILE = "stream.file";
  
  /** If the content stream should come directly from a field */
  public static final String STREAM_BODY = "stream.body";
  
  /** 
   * Explicity set the content type for the input stream
   * If multiple streams are specified, the explicit contentType
   * will be used for all of them.  
   */
  public static final String STREAM_CONTENTTYPE = "stream.contentType";
    
  /** 'true' if the header should include the handler name */
  public static final String HEADER_ECHO_HANDLER = "echoHandler";
  
  /** include the parameters in the header **/
  public static final String HEADER_ECHO_PARAMS = "echoParams";
  
  /** valid values for: <code>echoParams</code> */
  public enum EchoParamStyle {
    EXPLICIT,
    ALL,
    NONE;
    
    public static EchoParamStyle get( String v ) {
      if( v != null ) {
        v = v.toUpperCase();
        if( v.equals( "EXPLICIT" ) ) {
          return EXPLICIT;
        }
        if( v.equals( "ALL") ) {
          return ALL;
        }
        if( v.equals( "NONE") ) {  // the same as nothing...
          return NONE;
        }
      }
      return null;
    }
  };
  
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
    return val==null ? null : parseBool(val);
  }

  /** Returns the boolean value of the param, or def if not set */
  public boolean getBool(String param, boolean def) {
    String val = get(param);
    return val==null ? def : parseBool(val);
  }
  
  /** Returns the Boolean value of the field param, 
      or the value for param, or null if neither is set. */
  public Boolean getFieldBool(String field, String param) {
    String val = getFieldParam(field, param);
    return val==null ? null : parseBool(val);
  }
  
  /** Returns the boolean value of the field param, 
  or the value for param, or def if neither is set. */
  public boolean getFieldBool(String field, String param, boolean def) {
    String val = getFieldParam(field, param);
    return val==null ? def : parseBool(val);
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
  
  /** Returns the int value of the field param,
  or the value for param, or def if neither is set. */
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
  
  /** how to transform a String into a boolean... more flexible than
   * Boolean.parseBoolean() to enable easier integration with html forms.
   */
  protected boolean parseBool(String s) {
    if( s != null ) {
      if( s.startsWith("true") || s.startsWith("on") || s.startsWith("yes") ) {
        return true;
      }
      if( s.startsWith("false") || s.startsWith("off") || s.equals("no") ) {
        return false;
      }
    }
    throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "invalid boolean value: "+s );
  }

  /** Create a Map<String,String> from a NamedList given no keys are repeated */
  public static Map<String,String> toMap(NamedList params) {
    HashMap<String,String> map = new HashMap<String,String>();
    for (int i=0; i<params.size(); i++) {
      map.put(params.getName(i), params.getVal(i).toString());
    }
    return map;
  }

  /** Create a Map<String,String[]> from a NamedList */
  public static Map<String,String[]> toMultiMap(NamedList params) {
    HashMap<String,String[]> map = new HashMap<String,String[]>();
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
    HashMap<String,String> map = new HashMap<String,String>();
    for (int i=0; i<params.size(); i++) {
      String prev = map.put(params.getName(i), params.getVal(i).toString());
      if (prev!=null) return new MultiMapSolrParams(toMultiMap(params));
    }
    return new MapSolrParams(map);
  }
  
  /** Convert this to a NamedList */
  public NamedList<Object> toNamedList() {
    final SimpleOrderedMap<Object> result = new SimpleOrderedMap<Object>();
    
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








