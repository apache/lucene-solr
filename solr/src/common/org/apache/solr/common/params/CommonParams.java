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

import java.util.Locale;


/**
 * Parameters used across many handlers
 */
public interface CommonParams {

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
  
  /** version parameter to check request-response compatibility */
  public static final String VERSION ="version";
  
  /** query and init param for field list */
  public static final String FL = "fl";
  
  /** default query field */
  public static final String DF = "df";
  
  /** whether to include debug data */
  public static final String DEBUG_QUERY = "debugQuery";
  
  /** another query to explain against */
  public static final String EXPLAIN_OTHER = "explainOther";
  

  /** If the content stream should come from a URL (using URLConnection) */
  public static final String STREAM_URL = "stream.url";

  /** If the content stream should come from a File (using FileReader) */
  public static final String STREAM_FILE = "stream.file";
  
  /** If the content stream should come directly from a field */
  public static final String STREAM_BODY = "stream.body";
  
  /** 
   * Explicitly set the content type for the input stream
   * If multiple streams are specified, the explicit contentType
   * will be used for all of them.  
   */
  public static final String STREAM_CONTENTTYPE = "stream.contentType";
  
  /**
   * Timeout value in milliseconds.  If not set, or the value is <= 0, there is no timeout.
   */
  public static final String TIME_ALLOWED = "timeAllowed";
  
  /** 'true' if the header should include the handler name */
  public static final String HEADER_ECHO_HANDLER = "echoHandler";
  
  /** include the parameters in the header **/
  public static final String HEADER_ECHO_PARAMS = "echoParams";

  /** include header in the response */
  public static final String OMIT_HEADER = "omitHeader";

  /** valid values for: <code>echoParams</code> */
  public enum EchoParamStyle {
    EXPLICIT,
    ALL,
    NONE;
    
    public static EchoParamStyle get( String v ) {
      if( v != null ) {
        v = v.toUpperCase(Locale.ENGLISH);
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

  public static final String EXCLUDE = "ex";
  public static final String TAG = "tag";
  public static final String TERMS = "terms";
  public static final String OUTPUT_KEY = "key";
  public static final String FIELD = "f";
  public static final String VALUE = "v";
  public static final String TRUE = Boolean.TRUE.toString();
  public static final String FALSE = Boolean.FALSE.toString();
}

