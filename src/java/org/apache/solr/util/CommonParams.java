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

package org.apache.solr.util;

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrException;

import org.apache.solr.util.StrUtils;
import org.apache.solr.util.NamedList;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.io.IOException;

    

  /**
   * A collection on common params, both for Plugin initialization and
   * for Requests.
   */
  public class CommonParams {

    /** query and init param for field list */
    public static String FL = "fl";
    /** default query field */
    public static String DF = "df";
    /** whether to include debug data */
    public static String DEBUG_QUERY = "debugQuery";
    /** another query to explain against */
    public static String EXPLAIN_OTHER = "explainOther";
    /** wether to highlight */
    public static String HIGHLIGHT = "highlight";
    /** fields to highlight */
    public static String HIGHLIGHT_FIELDS = "highlightFields";
    /** maximum highlight fragments to return */
    public static String MAX_SNIPPETS = "maxSnippets";
    /** override default highlight Formatter class */
    public static String HIGHLIGHT_FORMATTER_CLASS = "highlightFormatterClass";


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
          SolrCore.log.severe("init param is not a str: " + FL);
        }
      }

      tmp = args.get(DF);
      if (null != tmp) {
        if (tmp instanceof String) {
          df = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + DF);
        }
      }

      tmp = args.get(DEBUG_QUERY);
      if (null != tmp) {
        if (tmp instanceof String) {
          debugQuery = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + DEBUG_QUERY);
        }
      }

      tmp = args.get(EXPLAIN_OTHER);
      if (null != tmp) {
        if (tmp instanceof String) {
          explainOther = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + EXPLAIN_OTHER);
        }
      }

      tmp = args.get(HIGHLIGHT);
      if (null != tmp) {
        if (tmp instanceof String) {
          // Any non-empty string other than 'false' implies highlighting
          String val = tmp.toString().trim();
          highlight = !(val.equals("") || val.equals("false"));
        } else {
          SolrCore.log.severe("init param is not a str: " + HIGHLIGHT);
        }
      }

      tmp = args.get(HIGHLIGHT_FIELDS);
      if (null != tmp) {
        if (tmp instanceof String) {
          highlightFields = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + HIGHLIGHT);
        }
      }

      tmp = args.get(MAX_SNIPPETS);
      if (null != tmp) {
        if (tmp instanceof Integer) {
          maxSnippets = ((Integer)tmp).intValue();
        } else {
          SolrCore.log.severe("init param is not an int: " + MAX_SNIPPETS);
        }
      }

      tmp = args.get(HIGHLIGHT_FORMATTER_CLASS);
      if (null != tmp) {
        if (tmp instanceof String) {
          highlightFormatterClass = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + HIGHLIGHT_FORMATTER_CLASS);
        }
      }
        
    }

  }

