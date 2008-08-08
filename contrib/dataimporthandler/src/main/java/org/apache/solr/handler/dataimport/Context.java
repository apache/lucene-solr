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

package org.apache.solr.handler.dataimport;

import org.apache.solr.core.SolrCore;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * This abstract class gives access to all available objects. So any
 * component implemented by a user can have the full power of DataImportHandler
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id$
 * @since solr 1.3
 */
public abstract class Context {
  public static final int FULL_DUMP = 1, DELTA_DUMP = 2, FIND_DELTA = 3;

  public static final String SCOPE_ENTITY = "entity", SCOPE_GLOBAL = "global",
          SCOPE_DOC = "document";

  /**
   * Get the value of any attribute put into this entity
   *
   * @param name name of the attribute eg: 'name'
   * @return value of named attribute in entity
   */
  public abstract String getEntityAttribute(String name);

  /**
   * Returns all the fields put into an entity. each item (which is a map ) in
   * the list corresponds to one field. each if the map contains the attribute
   * names and values in a field
   *
   * @return all fields in an entity
   */
  public abstract List<Map<String, String>> getAllEntityFields();

  /**
   * Returns the VariableResolver used in this entity which can be used to
   * resolve the tokens in ${<namespce.name>}
   *
   * @return a VariableResolver instance
   * @see org.apache.solr.handler.dataimport.VariableResolver
   */

  public abstract VariableResolver getVariableResolver();

  /**
   * Gets the datasource instance defined for this entity.
   *
   * @return a new DataSource instance as configured for the current entity
   * @see org.apache.solr.handler.dataimport.DataSource
   */
  public abstract DataSource getDataSource();

  /**
   * Gets a new DataSource instance with a name.
   *
   * @param name Name of the dataSource as defined in the dataSource tag
   * @return a new DataSource instance as configured for the named entity
   * @see org.apache.solr.handler.dataimport.DataSource
   */
  public abstract DataSource getDataSource(String name);

  /**
   * Returns the instance of EntityProcessor used for this entity
   *
   * @return instance of EntityProcessor used for the current entity
   * @see org.apache.solr.handler.dataimport.EntityProcessor
   */
  public abstract EntityProcessor getEntityProcessor();

  /**
   * Store values in a certain name and scope (entity, document,global)
   *
   * @param name  the key
   * @param val   the value
   * @param scope the scope in which the given key, value pair is to be stored
   */
  public abstract void setSessionAttribute(String name, Object val, String scope);

  /**
   * get a value by name in the given scope (entity, document,global)
   *
   * @param name  the key
   * @param scope the scope from which the value is to be retreived
   * @return the object stored in the given scope with the given key
   */
  public abstract Object getSessionAttribute(String name, String scope);

  /**
   * Get the context instance for the parent entity. works only in the full dump
   * If the current entity is rootmost a null is returned
   *
   * @return parent entity's Context
   */
  public abstract Context getParentContext();

  /**
   * The request parameters passed over HTTP for this command the values in the
   * map are either String(for single valued parameters) or List<String> (for
   * multi-valued parameters)
   *
   * @return the request parameters passed in the URL to initiate this process
   */
  public abstract Map<String, Object> getRequestParameters();

  /**
   * Returns if the current entity is the root entity
   *
   * @return true if current entity is the root entity, false otherwise
   */
  public abstract boolean isRootEntity();

  /**
   * Returns the current process FULL_DUMP =1, DELTA_DUMP=2, FIND_DELTA=3
   *
   * @return the code of the current running process
   */
  public abstract int currentProcess();

  /**
   * Exposing the actual SolrCore to the components
   *
   * @return the core
   */
  public abstract SolrCore getSolrCore();
}
