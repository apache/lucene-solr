/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.request;

import org.apache.solr.util.NamedList;
import org.apache.solr.util.StrUtils;

import javax.servlet.ServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.io.IOException;

/**  SolrParams hold request parameters.
 *
 * @author yonik
 * @version $Id$
 */
public abstract class SolrParams {
  /** the query type - which query handler should handle the request */
  public static final String QT ="qt";
  /** the response writer type - the format of the response */
  public static final String WT ="wt";
  /** query string */
  public static final String Q ="q";
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
  /** wether to highlight */
  public static final String HIGHLIGHT = "highlight";
  /** fields to highlight */
  public static final String HIGHLIGHT_FIELDS = "highlightFields";
  /** maximum highlight fragments to return */
  public static final String MAX_SNIPPETS = "maxSnippets";
  /** override default highlight Formatter class */
  public static final String HIGHLIGHT_FORMATTER_CLASS = "highlightFormatterClass";

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
   * Numeric option indicating the maximum number of facet field counts
   * be included in the response for each field - in descending order of count.
   * Can be overriden on a per field basis.
   */
  public static final String FACET_LIMIT = "facet.limit";
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


  /** returns the String value of a param, or null if not set */
  public abstract String get(String param);

  /** returns an array of the String values of a param, or null if none */
  public abstract String[] getParams(String param);


  /** returns the value of the param, or def if not set */
  public String get(String param, String def) {
    String val = get(param);
    return val==null ? def : val;
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
    return val==null ? null : Integer.parseInt(val);
  }

  /** Returns the int value of the param, or def if not set */
  public int getInt(String param, int def) {
    String val = get(param);
    return val==null ? def : Integer.parseInt(val);
  }

  /** Returns the Float value of the param, or null if not set */
  public Float getFloat(String param) {
    String val = get(param);
    return val==null ? null : Float.parseFloat(val);
  }

  /** Returns the float value of the param, or def if not set */
  public float getFloat(String param, float def) {
    String val = get(param);
    return val==null ? def : Float.parseFloat(val);
  }

  /** how to transform a String into a boolean... more flexible than
   * Boolean.parseBoolean() to enable easier integration with html forms.
   */
  protected boolean parseBool(String s) {
    return s.startsWith("true") || s.startsWith("on") || s.startsWith("yes");
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
}


