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
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public abstract class Context {
  public static final String FULL_DUMP = "FULL_DUMP", DELTA_DUMP = "DELTA_DUMP", FIND_DELTA = "FIND_DELTA";

  /**
   * An object stored in entity scope is valid only for the current entity for the current document only.
   */
  public static final String SCOPE_ENTITY = "entity";

  /**
   * An object stored in global scope is available for the current import only but across entities and documents.
   */
  public static final String SCOPE_GLOBAL = "global";

  /**
   * An object stored in document scope is available for the current document only but across entities.
   */
  public static final String SCOPE_DOC = "document";

  /**
   * An object stored in 'solrcore' scope is available across imports, entities and documents throughout the life of
   * a solr core. A solr core unload or reload will destroy this data.
   */
  public static final String SCOPE_SOLR_CORE = "solrcore";

  /**
   * Get the value of any attribute put into this entity
   *
   * @param name name of the attribute eg: 'name'
   * @return value of named attribute in entity
   */
  public abstract String getEntityAttribute(String name);

  /**
   * Get the value of any attribute put into this entity after resolving all variables found in the attribute value
   * @param name name of the attribute
   * @return value of the named attribute after resolving all variables
   */
  public abstract String getResolvedEntityAttribute(String name);

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
   * resolve the tokens in ${&lt;namespce.name&gt;}
   *
   * @return a VariableResolver instance
   * @see org.apache.solr.handler.dataimport.VariableResolver
   */

  public abstract VariableResolver getVariableResolver();

  /**
   * Gets the datasource instance defined for this entity. Do not close() this instance.
   * Transformers should use the getDataSource(String name) method.
   *
   * @return a new DataSource instance as configured for the current entity
   * @see org.apache.solr.handler.dataimport.DataSource
   * @see #getDataSource(String)
   */
  @SuppressWarnings({"rawtypes"})
  public abstract DataSource getDataSource();

  /**
   * Gets a new DataSource instance with a name. Ensure that you close() this after use
   * because this is created just for this method call.
   *
   * @param name Name of the dataSource as defined in the dataSource tag
   * @return a new DataSource instance
   * @see org.apache.solr.handler.dataimport.DataSource
   */
  @SuppressWarnings({"rawtypes"})
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
   * @param scope the scope from which the value is to be retrieved
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
   * map are either String(for single valued parameters) or List&lt;String&gt; (for
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
   * Returns the current process FULL_DUMP, DELTA_DUMP, FIND_DELTA
   *
   * @return the type of the current running process
   */
  public abstract String currentProcess();

  /**
   * Exposing the actual SolrCore to the components
   *
   * @return the core
   */
  public abstract SolrCore getSolrCore();

  /**
   * Makes available some basic running statistics such as "docCount",
   * "deletedDocCount", "rowCount", "queryCount" and "skipDocCount"
   *
   * @return a Map containing running statistics of the current import
   */
  public abstract Map<String, Object> getStats();

  /**
   * Returns the text specified in the script tag in the data-config.xml 
   */
  public abstract String getScript();

  /**
   * Returns the language of the script as specified in the script tag in data-config.xml
   */
  public abstract String getScriptLanguage();

  /**delete a document by id
   */
  public abstract void deleteDoc(String id);

  /**delete documents by query
   */
  public abstract void deleteDocByQuery(String query);

  /**Use this directly to  resolve variable
   * @param var the variable name 
   * @return the resolved value
   */
  public abstract Object resolve(String var);

  /** Resolve variables in a template
   *
   * @return The string w/ variables resolved
   */
  public abstract String replaceTokens(String template);

}
