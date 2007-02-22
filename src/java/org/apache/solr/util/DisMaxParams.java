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
   * A collection of params used in DisMaxRequestHandler,
   both for Plugin initialization and * for Requests.
   */
  public  class DisMaxParams extends CommonParams {

    /** query and init param for tiebreaker value */
    public static String TIE = "tie";
    /** query and init param for query fields */
    public static String QF = "qf";
    /** query and init param for phrase boost fields */
    public static String PF = "pf";
    /** query and init param for MinShouldMatch specification */
    public static String MM = "mm";
    /**
     * query and init param for Phrase Slop value in phrase
     * boost query (in pf fields)
     */
    public static String PS = "ps";
    /**
     * query and init param for phrase Slop value in phrases
     * explicitly included in the user's query string ( in qf fields)
     */
    public static String QS = "qs";
    /** query and init param for boosting query */
    public static String BQ = "bq";
    /** query and init param for boosting functions */
    public static String BF = "bf";
    /**
     * Alternate query (expressed in Solr QuerySyntax)
     * to use if main query (q) is empty
     */
    public static String ALTQ = "q.alt";
    /** query and init param for filtering query
     * @deprecated use SolrParams.FQ or SolrPluginUtils.parseFilterQueries
     */
    public static String FQ = "fq";
    /** query and init param for field list */
    public static String GEN = "gen";
        
    /**
     * the default tie breaker to use in DisjunctionMaxQueries
     * @deprecated - use explicit default with SolrParams.getFloat
     */
    public float tiebreaker = 0.0f;
    /**
     * the default query fields to be used
     * @deprecated - use explicit default with SolrParams.get
     */
    public String qf = null;
    /**
     * the default phrase boosting fields to be used
     * @deprecated - use explicit default with SolrParams.get
     */
    public String pf = null;
    /**
     * the default min should match to be used
     * @deprecated - use explicit default with SolrParams.get
     */
    public String mm = "100%";
    /**
     * the default phrase slop to be used 
     * @deprecated - use explicit default with SolrParams.getInt
     */
    public int pslop = 0;
    /**
     * the default boosting query to be used
     * @deprecated - use explicit default with SolrParams.get
     */
    public String bq = null;
    /**
     * the default boosting functions to be used
     * @deprecated - use explicit default with SolrParams.get
     */
    public String bf = null;
    /**
     * the default filtering query to be used
     * @deprecated - use explicit default with SolrParams.get
     */
    public String fq = null;


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
     * @deprecated use SolrParams.toSolrParams
     */
    public void setValues(NamedList args) {

      super.setValues(args);

      Object tmp;

      tmp = args.get(TIE);
      if (null != tmp) {
        if (tmp instanceof Float) {
          tiebreaker = ((Float)tmp).floatValue();
        } else {
          SolrCore.log.severe("init param is not a float: " + TIE);
        }
      }

      tmp = args.get(QF);
      if (null != tmp) {
        if (tmp instanceof String) {
          qf = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + QF);
        }
      }

      tmp = args.get(PF);
      if (null != tmp) {
        if (tmp instanceof String) {
          pf = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + PF);
        }
      }

        
      tmp = args.get(MM);
      if (null != tmp) {
        if (tmp instanceof String) {
          mm = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + MM);
        }
      }
        
      tmp = args.get(PS);
      if (null != tmp) {
        if (tmp instanceof Integer) {
          pslop = ((Integer)tmp).intValue();
        } else {
          SolrCore.log.severe("init param is not an int: " + PS);
        }
      }

      tmp = args.get(BQ);
      if (null != tmp) {
        if (tmp instanceof String) {
          bq = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + BQ);
        }
      }
 
      tmp = args.get(BF);
      if (null != tmp) {
        if (tmp instanceof String) {
          bf = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + BF);
        }
      }
 
      tmp = args.get(FQ);
      if (null != tmp) {
        if (tmp instanceof String) {
          fq = tmp.toString();
        } else {
          SolrCore.log.severe("init param is not a str: " + FQ);
        }
      }
                
    }

  }
