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

import java.io.Closeable;
import java.util.Map;

/**
 * <p>
 * An instance of entity processor serves an entity. It is reused throughout the
 * import process.
 * </p>
 * <p>
 * Implementations of this abstract class must provide a public no-args constructor.
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 * @since solr 1.3
 */
public abstract class EntityProcessor implements Closeable {

  /**
   * This method is called when it starts processing an entity. When it comes
   * back to the entity it is called again. So it can reset anything at that point.
   * For a rootmost entity this is called only once for an ingestion. For sub-entities , this
   * is called multiple once for each row from its parent entity
   *
   * @param context The current context
   */
  public abstract void init(Context context);

  /**
   * This method helps streaming the data for each row . The implementation
   * would fetch as many rows as needed and gives one 'row' at a time. Only this
   * method is used during a full import
   *
   * @return A 'row'.  The 'key' for the map is the column name and the 'value'
   *         is the value of that column. If there are no more rows to be
   *         returned, return 'null'
   */
  public abstract Map<String, Object> nextRow();

  /**
   * This is used for delta-import. It gives the pks of the changed rows in this
   * entity
   *
   * @return the pk vs value of all changed rows
   */
  public abstract Map<String, Object> nextModifiedRowKey();

  /**
   * This is used during delta-import. It gives the primary keys of the rows
   * that are deleted from this entity. If this entity is the root entity, solr
   * document is deleted. If this is a sub-entity, the Solr document is
   * considered as 'changed' and will be recreated
   *
   * @return the pk vs value of all changed rows
   */
  public abstract Map<String, Object> nextDeletedRowKey();

  /**
   * This is used during delta-import. This gives the primary keys and their
   * values of all the rows changed in a parent entity due to changes in this
   * entity.
   *
   * @return the pk vs value of all changed rows in the parent entity
   */
  public abstract Map<String, Object> nextModifiedParentRowKey();

  /**
   * Invoked for each entity at the very end of the import to do any needed cleanup tasks.
   * 
   */
  public abstract void destroy();

  /**
   * Invoked after the transformers are invoked. EntityProcessors can add, remove or modify values
   * added by Transformers in this method.
   *
   * @param r The transformed row
   * @since solr 1.4
   */
  public void postTransform(Map<String, Object> r) {
  }

  /**
   * Invoked when the Entity processor is destroyed towards the end of import.
   *
   * @since solr 1.4
   */
  public void close() {
    //no-op
  }
}
