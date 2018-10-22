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
package org.apache.solr.client.solrj.response;

import org.apache.solr.common.SolrDocumentList;

import java.io.Serializable;

/**
 * Represents a group. A group contains a common group value that all documents inside the group share and
 * documents that belong to this group.
 *
 * A group value can be a field value, function result or a query string depending on the {@link GroupCommand}.
 * In case of a field value or a function result the value is always a indexed value.
 *
 * @since solr 3.4
 */
public class Group implements Serializable {

  private final String _groupValue;
  private final SolrDocumentList _result;

  /**
   * Creates a Group instance.
   *
   * @param groupValue The common group value (indexed value) that all documents share.
   * @param result The documents to be displayed that belong to this group
   */
  public Group(String groupValue, SolrDocumentList result) {
    _groupValue = groupValue;
    _result = result;
  }

  /**
   * Returns the common group value that all documents share inside this group.
   * This is an indexed value, not a stored value.
   *
   * @return the common group value
   */
  public String getGroupValue() {
    return _groupValue;
  }

  /**
   * Returns the documents to be displayed that belong to this group.
   * How many documents are returned depend on the <code>group.offset</code> and <code>group.limit</code> parameters.
   *
   * @return the documents to be displayed that belong to this group
   */
  public SolrDocumentList getResult() {
    return _result;
  }

}