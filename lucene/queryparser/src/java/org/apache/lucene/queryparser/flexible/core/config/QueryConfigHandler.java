package org.apache.lucene.queryparser.flexible.core.config;

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

import java.util.LinkedList;

import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;

/**
 * This class can be used to hold any query configuration and no field
 * configuration. For field configuration, it creates an empty
 * {@link FieldConfig} object and delegate it to field config listeners, 
 * these are responsible for setting up all the field configuration.
 * 
 * {@link QueryConfigHandler} should be extended by classes that intends to
 * provide configuration to {@link QueryNodeProcessor} objects.
 * 
 * The class that extends {@link QueryConfigHandler} should also provide
 * {@link FieldConfig} objects for each collection field.
 * 
 * @see FieldConfig
 * @see FieldConfigListener
 * @see QueryConfigHandler
 */
public abstract class QueryConfigHandler extends AbstractQueryConfig {
  
  final private LinkedList<FieldConfigListener> listeners = new LinkedList<FieldConfigListener>();

  /**
   * Returns an implementation of
   * {@link FieldConfig} for a specific field name. If the implemented
   * {@link QueryConfigHandler} does not know a specific field name, it may
   * return <code>null</code>, indicating there is no configuration for that
   * field.
   * 
   * @param fieldName
   *          the field name
   * @return a {@link FieldConfig} object containing the field name
   *         configuration or <code>null</code>, if the implemented
   *         {@link QueryConfigHandler} has no configuration for that field
   */
  public FieldConfig getFieldConfig(String fieldName) {
    FieldConfig fieldConfig = new FieldConfig(StringUtils.toString(fieldName));

    for (FieldConfigListener listener : this.listeners) {
      listener.buildFieldConfig(fieldConfig);
    }

    return fieldConfig;

  }

  /**
   * Adds a listener. The added listeners are called in the order they are
   * added.
   * 
   * @param listener
   *          the listener to be added
   */
  public void addFieldConfigListener(FieldConfigListener listener) {
    this.listeners.add(listener);
  }
  
}
