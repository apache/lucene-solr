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

import java.util.*;

/**
 * <code>SolrQueryResponse</code> is used by a query handler to return
 * the response to a query.
 * @author yonik
 * @version $Id$
 * @since solr 0.9
 */

public class SolrQueryResponse {

  protected  NamedList values = new NamedList();
  // current holder for user defined values

  protected Set<String> defaultReturnFields;

  // error if this is set...
  protected Exception err;

  /***
   // another way of returning an error
  int errCode;
  String errMsg;
  ***/

  public NamedList getValues() { return values; }

  /**
   *  Sets a list of all the named values to return.
   */
  public void setAllValues(NamedList nameValuePairs) {
    values=nameValuePairs;
  }

  /**
   * Sets the document field names of fields to return by default.
   */
  public void setReturnFields(Set<String> fields) {
    defaultReturnFields=fields;
  }
  // TODO: should this be represented as a String[] such
  // that order can be maintained if needed?

  /**
   * The document field names to return by default.
   */
  public Set<String> getReturnFields() {
    return defaultReturnFields;
  }


  /**
   * Appends a named value to the list of named values to be returned.
   * @param name  the name of the value - may be null if unnamed
   * @param val   the value to add - also may be null since null is a legal value
   */
  public void add(String name, Object val) {
    values.add(name,val);
  }

  /**
   * Causes an error to be returned instead of the results.
   */
  public void setException(Exception e) {
    err=e;
  }

  /**
   * Returns an Exception if there was a fatal error in processing the request.
   * Returns null if the request succeeded.
   */
  public Exception getException() {
    return err;
  }

  // Get and Set the endtime in milliseconds... used
  // to calculate query time.
  protected long endtime;

  /** Time in milliseconds when the response officially finished. 
   */
  public long getEndTime() {
    if (endtime==0) {
      setEndTime();
    }
    return endtime;
  }

  /**
   * Stop the timer for how long this query took.
   */
  public long setEndTime() {
    return setEndTime(System.currentTimeMillis());
  }

  public long setEndTime(long endtime) {
    if (endtime!=0) {
      this.endtime=endtime;
    }
    return this.endtime;
  }


}
