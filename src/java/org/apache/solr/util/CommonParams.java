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

import org.apache.solr.common.util.NamedList;

import java.util.logging.Logger;

/**
 * A collection on common params, both for Plugin initialization and
 * for Requests.
 */
@Deprecated
public class CommonParams implements org.apache.solr.common.params.CommonParams {

  public static Logger log = Logger.getLogger(CommonParams.class.getName());
  

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

    tmp = args.get(FL);
    if (null != tmp) {
      if (tmp instanceof String) {
        fl = tmp.toString();
      } else {
        log.severe("init param is not a str: " + FL);
      }
    }

    tmp = args.get(DF);
    if (null != tmp) {
      if (tmp instanceof String) {
        df = tmp.toString();
      } else {
        log.severe("init param is not a str: " + DF);
      }
    }

    tmp = args.get(DEBUG_QUERY);
    if (null != tmp) {
      if (tmp instanceof String) {
        debugQuery = tmp.toString();
      } else {
        log.severe("init param is not a str: " + DEBUG_QUERY);
      }
    }

    tmp = args.get(EXPLAIN_OTHER);
    if (null != tmp) {
      if (tmp instanceof String) {
        explainOther = tmp.toString();
      } else {
        log.severe("init param is not a str: " + EXPLAIN_OTHER);
      }
    }

  }

}

