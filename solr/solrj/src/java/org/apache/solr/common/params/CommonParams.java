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

import java.util.Locale;


/**
 * Parameters used across many handlers
 */
public interface CommonParams {

  /** 
   * Override for the concept of "NOW" to be used throughout this request, 
   * expressed as milliseconds since epoch.  This is primarily used in 
   * distributed search to ensure consistent time values are used across 
   * multiple sub-requests.
   */
  public static final String NOW = "NOW";

  /** 
   * Specifies the TimeZone used by the client for the purposes of 
   * any DateMath rounding that may take place when executing the request
   */
  public static final String TZ = "TZ";

  /** the Request Handler (formerly known as the Query Type) - which Request Handler should handle the request */
  public static final String QT ="qt";
  
  /** the response writer type - the format of the response */
  public static final String WT ="wt";
  
  /** query string */
  public static final String Q ="q";

  /** rank query */
  public static final String RQ ="rq";
  
  /** distrib string */
  public static final String DISTRIB = "distrib";
  
  /** sort order */
  public static final String SORT ="sort";
  
  /** Lucene query string(s) for filtering the results without affecting scoring */
  public static final String FQ ="fq";
  
  /** zero based offset of matching documents to retrieve */
  public static final String START ="start";
  public static final int START_DEFAULT = 0;
  
  /** number of documents to return starting at "start" */
  public static final String ROWS ="rows";
  public static final int ROWS_DEFAULT = 10;

  // SOLR-4228 start
  /** handler value for SolrPing */
  public static final String PING_HANDLER = "/admin/ping";
  
  /** "action" parameter for SolrPing */
  public static final String ACTION = "action";
  
  /** "disable" value for SolrPing action */
  public static final String DISABLE = "disable";
  
  /** "enable" value for SolrPing action */
  public static final String ENABLE = "enable";
  
  /** "ping" value for SolrPing action */
  public static final String PING = "ping";
  // SOLR-4228 end

  /** stylesheet to apply to XML results */
  public static final String XSL ="xsl";
  
  /** version parameter to check request-response compatibility */
  public static final String VERSION ="version";
  
  /** query and init param for field list */
  public static final String FL = "fl";
  
  /** default query field */
  public static final String DF = "df";

  /** Transformer param -- used with XSLT */
  public static final String TR = "tr";
  
  /** whether to include debug data for all components pieces, including doing explains*/
  public static final String DEBUG_QUERY = "debugQuery";

  /**
   * Whether to provide debug info for specific items.
   *
   * @see #DEBUG_QUERY
   */
  public static final String DEBUG = "debug";

  /**
   * {@link #DEBUG} value indicating an interest in debug output related to timing
   */
  public static final String TIMING = "timing";
  /**
   * {@link #DEBUG} value indicating an interest in debug output related to the results (explains)
   */
  public static final String RESULTS = "results";
  /**
   * {@link #DEBUG} value indicating an interest in debug output related to the Query (parsing, etc.)
   */
  public static final String QUERY = "query";
  /**
   * {@link #DEBUG} value indicating an interest in debug output related to the distributed tracking
   */
  public static final String TRACK = "track";
  /** 
   * boolean indicating whether score explanations should structured (true), 
   * or plain text (false)
   */
  public static final String EXPLAIN_STRUCT = "debug.explain.structured";
  
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
   * Timeout value in milliseconds.  If not set, or the value is &gt;= 0, there is no timeout.
   */
  public static final String TIME_ALLOWED = "timeAllowed";
  
  /** 'true' if the header should include the handler name */
  public static final String HEADER_ECHO_HANDLER = "echoHandler";
  
  /** include the parameters in the header **/
  public static final String HEADER_ECHO_PARAMS = "echoParams";

  /** include header in the response */
  public static final String OMIT_HEADER = "omitHeader";
  public static final String CORES_HANDLER_PATH = "/admin/cores";
  public static final String COLLECTIONS_HANDLER_PATH = "/admin/collections";
  public static final String INFO_HANDLER_PATH = "/admin/info";
  public static final String CONFIGSETS_HANDLER_PATH = "/admin/configs";
  public static final String AUTHZ_PATH = "/admin/authorization";
  public static final String AUTHC_PATH = "/admin/authentication";
  public static final String ZK_PATH = "/admin/zookeeper";

  /** valid values for: <code>echoParams</code> */
  public enum EchoParamStyle {
    EXPLICIT,
    ALL,
    NONE;
    
    public static EchoParamStyle get( String v ) {
      if( v != null ) {
        v = v.toUpperCase(Locale.ROOT);
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

  /** which parameters to log (if not supplied all parameters will be logged) **/
  public static final String LOG_PARAMS_LIST = "logParamsList";

  public static final String EXCLUDE = "ex";
  public static final String TAG = "tag";
  public static final String TERMS = "terms";
  public static final String OUTPUT_KEY = "key";
  public static final String FIELD = "f";
  public static final String VALUE = "v";
  public static final String THREADS = "threads";
  public static final String TRUE = Boolean.TRUE.toString();
  public static final String FALSE = Boolean.FALSE.toString();

  /** Used as a local parameter on queries.  cache=false means don't check any query or filter caches.
   * cache=true is the default.
   */
  public static final String CACHE = "cache";

  /** Used as a local param on filter queries in conjunction with cache=false.  Filters are checked in order, from
   * smallest cost to largest. If cost&gt;=100 and the query implements PostFilter, then that interface will be used to do post query filtering.
   */
  public static final String COST = "cost";

  /**
   * Request ID parameter added to the request when using debug=track
   */
  public static final String REQUEST_ID = "rid";

  /**
   * Request Purpose parameter added to each internal shard request when using debug=track
   */
  public static final String REQUEST_PURPOSE = "requestPurpose";

  /**
   * When querying a node, prefer local node's cores for distributed queries.
   */
  public static final String PREFER_LOCAL_SHARDS = "preferLocalShards";

  public static final String JAVABIN = "javabin";

  public static final String JSON = "json";

  public static final String PATH = "path";

  public static final String NAME = "name";
  public static final String VALUE_LONG = "val";
}

