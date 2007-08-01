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

import java.util.logging.Logger;

import org.apache.solr.common.util.NamedList;

/**
 * This class is scheduled for deletion.  Please update your code to the moved package.
 */
@Deprecated
public class DisMaxParams extends CommonParams implements org.apache.solr.common.params.DisMaxParams {

  public static Logger log = Logger.getLogger(DisMaxParams.class.getName());


  /** query and init param for filtering query
   * @deprecated use SolrParams.FQ or SolrPluginUtils.parseFilterQueries
   */
  public static String FQ = "fq";
  
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
        log.severe("init param is not a float: " + TIE);
      }
    }

    tmp = args.get(QF);
    if (null != tmp) {
      if (tmp instanceof String) {
        qf = tmp.toString();
      } else {
        log.severe("init param is not a str: " + QF);
      }
    }

    tmp = args.get(PF);
    if (null != tmp) {
      if (tmp instanceof String) {
        pf = tmp.toString();
      } else {
        log.severe("init param is not a str: " + PF);
      }
    }

        
    tmp = args.get(MM);
    if (null != tmp) {
      if (tmp instanceof String) {
        mm = tmp.toString();
      } else {
        log.severe("init param is not a str: " + MM);
      }
    }
        
    tmp = args.get(PS);
    if (null != tmp) {
      if (tmp instanceof Integer) {
        pslop = ((Integer)tmp).intValue();
      } else {
        log.severe("init param is not an int: " + PS);
      }
    }

    tmp = args.get(BQ);
    if (null != tmp) {
      if (tmp instanceof String) {
        bq = tmp.toString();
      } else {
        log.severe("init param is not a str: " + BQ);
      }
    }
 
    tmp = args.get(BF);
    if (null != tmp) {
      if (tmp instanceof String) {
        bf = tmp.toString();
      } else {
        log.severe("init param is not a str: " + BF);
      }
    }
 
    tmp = args.get(FQ);
    if (null != tmp) {
      if (tmp instanceof String) {
        fq = tmp.toString();
      } else {
        log.severe("init param is not a str: " + FQ);
      }
    }
                
  }

}
