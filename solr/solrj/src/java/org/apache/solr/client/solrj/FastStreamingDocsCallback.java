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

package org.apache.solr.client.solrj;


import org.apache.solr.common.util.DataEntry;

public interface FastStreamingDocsCallback {
  /** callback for a doclist
   *
   * @return the object to be shared with all the {{@link #startDoc(Object)}} calls. return null if nothing needs to be shared
   */
  default Object initDocList(Long numFound, Long start, Float maxScore) {
    return null;
  }


  /**
   * Started a document
   *
   * @param docListObj This object is the value returned by the {{@link #initDocList(Long, Long, Float)}} method
   * @return any arbitrary object that should be shared between each field
   */
  Object startDoc(Object docListObj);

  /**
   * FOund a new field
   *
   * @param field  Read the appropriate value
   * @param docObj The object returned by {{@link #startDoc(Object)}} method
   */
  void field(DataEntry field, Object docObj);

  /**
   * A document ends
   *
   * @param docObj The object returned by {{@link #startDoc(Object)}} method
   */
  default void endDoc(Object docObj) { }

  /** A new child doc starts
   * @param parentDocObj an objec that will be shared across all the  {{@link FastStreamingDocsCallback#field(DataEntry, Object)}}
   * @return any custom object that be shared with the fields in this child doc
   */
  default Object startChildDoc(Object parentDocObj) {
    return null;
  }



}
