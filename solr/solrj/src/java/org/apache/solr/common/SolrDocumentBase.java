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
package org.apache.solr.common;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class SolrDocumentBase<T, K> implements Map<String, T>, Serializable, MapWriter {

  /** Get all field names.
  */
  public abstract Collection<String> getFieldNames();

  /** Set a field with implied null value for boost.
   * @param name name of the field to set
   * @param value value of the field
   */
  public abstract void setField(String name, Object value);

  /** 
   * Add a field to the document.
   * @param name Name of the field, should match one of the field names defined under "fields" tag in schema.xml.
   * @param value Value of the field, should be of same class type as defined by "type" attribute of the corresponding field in schema.xml. 
   */
  public abstract void addField(String name, Object value); 

  /**
   * Get the first value or collection of values for a given field.
   */
  public abstract Object getFieldValue(String name);

  /**
   * Get a collection of values for a given field name
   */
  @SuppressWarnings({"rawtypes"})
  public abstract Collection getFieldValues(String name);

  public abstract void addChildDocument(K child);

  public abstract void addChildDocuments(Collection<K> children);

  /**
   * Returns the list of <em>anonymous</em> child documents, or null if none.
   * There may be other "labelled" child documents found in field values, in which the field name is the label.
   * This may be deprecated in 8.0.
   */
  public abstract List<K> getChildDocuments();

  /** Has <em>anonymous</em> children? */
  public abstract boolean hasChildDocuments();

  /**
   * The <em>anonymous</em> child document count.
   */
  @Deprecated
  public abstract int getChildDocumentCount();

}
