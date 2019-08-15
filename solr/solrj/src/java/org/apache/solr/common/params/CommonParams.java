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
import java.util.Set;


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
  String NOW = "NOW";

  /** 
   * Specifies the TimeZone used by the client for the purposes of 
   * any DateMath rounding that may take place when executing the request
   */
  String TZ = "TZ";

  /** the Request Handler (formerly known as the Query Type) - which Request Handler should handle the request */
  String QT ="qt";
  
  /** the response writer type - the format of the response */
  String WT ="wt";
  
  /** query string */
  String Q ="q";

  /** rank query */
  String RQ ="rq";
  
  /** distrib string */
  String DISTRIB = "distrib";
  
  /** sort order */
  String SORT ="sort";
  
  /** Lucene query string(s) for filtering the results without affecting scoring */
  String FQ ="fq";
  
  /** zero based offset of matching documents to retrieve */
  String START ="start";
  int START_DEFAULT = 0;
  
  /** number of documents to return starting at "start" */
  String ROWS ="rows";
  int ROWS_DEFAULT = 10;

  // SOLR-4228 start
  /** handler value for SolrPing */
  String PING_HANDLER = "/admin/ping";
  
  /** "action" parameter for SolrPing */
  String ACTION = "action";
  
  /** "disable" value for SolrPing action */
  String DISABLE = "disable";
  
  /** "enable" value for SolrPing action */
  String ENABLE = "enable";
  
  /** "ping" value for SolrPing action */
  String PING = "ping";
  // SOLR-4228 end

  /** stylesheet to apply to XML results */
  String XSL ="xsl";
  
  /** version parameter to check request-response compatibility */
  String VERSION ="version";
  
  /** query and init param for field list */
  String FL = "fl";
  
  /** default query field */
  String DF = "df";

  /** Transformer param -- used with XSLT */
  String TR = "tr";
  
  /** whether to include debug data for all components pieces, including doing explains*/
  String DEBUG_QUERY = "debugQuery";

  /**
   * Whether to provide debug info for specific items.
   *
   * @see #DEBUG_QUERY
   */
  String DEBUG = "debug";

  /**
   * {@link #DEBUG} value indicating an interest in debug output related to timing
   */
  String TIMING = "timing";
  /**
   * {@link #DEBUG} value indicating an interest in debug output related to the results (explains)
   */
  String RESULTS = "results";
  /**
   * {@link #DEBUG} value indicating an interest in debug output related to the Query (parsing, etc.)
   */
  String QUERY = "query";
  /**
   * {@link #DEBUG} value indicating an interest in debug output related to the distributed tracking
   */
  String TRACK = "track";
  /** 
   * boolean indicating whether score explanations should structured (true), 
   * or plain text (false)
   */
  String EXPLAIN_STRUCT = "debug.explain.structured";
  
  /** another query to explain against */
  String EXPLAIN_OTHER = "explainOther";
  

  /** If the content stream should come from a URL (using URLConnection) */
  String STREAM_URL = "stream.url";

  /** If the content stream should come from a File (using FileReader) */
  String STREAM_FILE = "stream.file";
  
  /** If the content stream should come directly from a field */
  String STREAM_BODY = "stream.body";
  
  /** 
   * Explicitly set the content type for the input stream
   * If multiple streams are specified, the explicit contentType
   * will be used for all of them.  
   */
  String STREAM_CONTENTTYPE = "stream.contentType";
  
  /**
   * Whether or not the search may be terminated early within a segment.
   */
  String SEGMENT_TERMINATE_EARLY = "segmentTerminateEarly";
  boolean SEGMENT_TERMINATE_EARLY_DEFAULT = false;

  /**
   * Timeout value in milliseconds.  If not set, or the value is &gt;= 0, there is no timeout.
   */
  String TIME_ALLOWED = "timeAllowed";
  
  /** 'true' if the header should include the handler name */
  String HEADER_ECHO_HANDLER = "echoHandler";
  
  /** include the parameters in the header **/
  String HEADER_ECHO_PARAMS = "echoParams";

  /** include header in the response */
  String OMIT_HEADER = "omitHeader";
  String CORES_HANDLER_PATH = "/admin/cores";
  String COLLECTIONS_HANDLER_PATH = "/admin/collections";
  String INFO_HANDLER_PATH = "/admin/info";
  String HEALTH_CHECK_HANDLER_PATH = INFO_HANDLER_PATH + "/health";
  String CONFIGSETS_HANDLER_PATH = "/admin/configs";
  String AUTHZ_PATH = "/admin/authorization";
  String AUTHC_PATH = "/admin/authentication";
  String ZK_PATH = "/admin/zookeeper";
  String ZK_STATUS_PATH = "/admin/zookeeper/status";
  String SYSTEM_INFO_PATH = "/admin/info/system";
  String METRICS_PATH = "/admin/metrics";
  String METRICS_HISTORY_PATH = "/admin/metrics/history";
  String AUTOSCALING_PATH = "/admin/autoscaling";
  String AUTOSCALING_HISTORY_PATH = "/admin/autoscaling/history";
  String AUTOSCALING_DIAGNOSTICS_PATH = "/admin/autoscaling/diagnostics";
  String AUTOSCALING_SUGGESTIONS_PATH = "/admin/autoscaling/suggestions";

  String STATUS = "status";

  String OK = "OK";
  String FAILURE = "FAILURE";

  Set<String> ADMIN_PATHS = Set.of(
      CORES_HANDLER_PATH,
      COLLECTIONS_HANDLER_PATH,
      HEALTH_CHECK_HANDLER_PATH,
      CONFIGSETS_HANDLER_PATH,
      SYSTEM_INFO_PATH,
      AUTHC_PATH,
      AUTHZ_PATH,
      METRICS_PATH,
      METRICS_HISTORY_PATH,
      AUTOSCALING_PATH,
      AUTOSCALING_HISTORY_PATH,
      AUTOSCALING_DIAGNOSTICS_PATH,
      AUTOSCALING_SUGGESTIONS_PATH);
  String APISPEC_LOCATION = "apispec/";
  String INTROSPECT = "/_introspect";

  /** valid values for: <code>echoParams</code> */
  enum EchoParamStyle {
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
  }

  /** which parameters to log (if not supplied all parameters will be logged) **/
  String LOG_PARAMS_LIST = "logParamsList";

  String EXCLUDE = "ex";
  String TAG = "tag";
  String TERMS = "terms";
  String OUTPUT_KEY = "key";
  String FIELD = "f";
  String VALUE = "v";
  String THREADS = "threads";
  String TRUE = Boolean.TRUE.toString();
  String FALSE = Boolean.FALSE.toString();

  /** document type in {@link CollectionAdminParams#SYSTEM_COLL} collection. **/
  String TYPE = "type";

  /** Used as a local parameter on queries.  cache=false means don't check any query or filter caches.
   * cache=true is the default.
   */
  String CACHE = "cache";

  /** Used as a local param on filter queries in conjunction with cache=false.  Filters are checked in order, from
   * smallest cost to largest. If cost&gt;=100 and the query implements PostFilter, then that interface will be used to do post query filtering.
   */
  String COST = "cost";

  /**
   * Request ID parameter added to the request when using debug=track
   */
  String REQUEST_ID = "rid";

  /**
   * Request Purpose parameter added to each internal shard request when using debug=track
   */
  String REQUEST_PURPOSE = "requestPurpose";

  /**
   * When querying a node, prefer local node's cores for distributed queries.
   * @deprecated Use {@code ShardParams.SHARDS_PREFERENCE}
   */
  @Deprecated
  String PREFER_LOCAL_SHARDS = "preferLocalShards";

  String JAVABIN = "javabin";

  String JSON = "json";

  String PATH = "path";

  String NAME = "name";
  String VALUE_LONG = "val";

  String VERSION_FIELD="_version_";

  String FAIL_ON_VERSION_CONFLICTS ="failOnVersionConflicts";

  String ID = "id";
  String JSON_MIME = "application/json";

  String JAVABIN_MIME = "application/javabin";

  String FILE = "file";
  String FILES = "files";

  String CHILDDOC = "_childDocuments_";

}

