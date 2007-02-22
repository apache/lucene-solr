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

package org.apache.solr.util;

import org.apache.solr.core.SolrCore;

import org.apache.solr.util.NamedList;
import org.apache.solr.request.SolrParams;



/**
 * A collection on common params, both for Plugin initialization and
 * for Requests.
 */
@Deprecated
public class CommonParams {

  @Deprecated
  public static String FL = "fl";
  /** default query field */
  @Deprecated
  public static String DF = "df";
  /** whether to include debug data */
  @Deprecated
  public static String DEBUG_QUERY = "debugQuery";
  /** another query to explain against */
  @Deprecated
  public static String EXPLAIN_OTHER = "explainOther";


  /** the default field list to be used */
  public String fl = null;
  /** the default field to query */
  public String df = null;
  /** do not debug by default **/
  public String debugQuery = null;
  /** no default other explanation query **/
  public String explainOther = null;
  /** whether to highlight */
  public boolean highlight = false;
  /** fields to highlight */
  public String highlightFields = null;
  /** maximum highlight fragments to return */
  public int maxSnippets = 1;
  /** override default highlight Formatter class */
  public String highlightFormatterClass = null;


  public CommonParams() {
    /* :NOOP: */
  }

  /** @see #setValues */
  public CommonParams(NamedList args) {
    this();
    setValues(args);
  }

  /**
   * Sets the params using values from a NamedList, usefull in the
   * init method for your handler.
   *
   * <p>
   * If any param is not of the expected type, a severe error is
   * logged,and the param is skipped.
   * </p>
   *
   * <p>
   * If any param is not of in the NamedList, it is skipped and the
   * old value is left alone.
   * </p>
   *
   */
  public void setValues(NamedList args) {

    Object tmp;

    tmp = args.get(SolrParams.FL);
    if (null != tmp) {
      if (tmp instanceof String) {
        fl = tmp.toString();
      } else {
        SolrCore.log.severe("init param is not a str: " + SolrParams.FL);
      }
    }

    tmp = args.get(SolrParams.DF);
    if (null != tmp) {
      if (tmp instanceof String) {
        df = tmp.toString();
      } else {
        SolrCore.log.severe("init param is not a str: " + SolrParams.DF);
      }
    }

    tmp = args.get(SolrParams.DEBUG_QUERY);
    if (null != tmp) {
      if (tmp instanceof String) {
        debugQuery = tmp.toString();
      } else {
        SolrCore.log.severe("init param is not a str: " + SolrParams.DEBUG_QUERY);
      }
    }

    tmp = args.get(SolrParams.EXPLAIN_OTHER);
    if (null != tmp) {
      if (tmp instanceof String) {
        explainOther = tmp.toString();
      } else {
        SolrCore.log.severe("init param is not a str: " + SolrParams.EXPLAIN_OTHER);
      }
    }

  }

}

