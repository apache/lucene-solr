package org.apache.lucene.queryparser.flexible.standard.config;

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

import java.util.Map;

import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfigListener;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

/**
 * This listener is used to listen to {@link FieldConfig} requests in
 * {@link QueryConfigHandler} and add {@link ConfigurationKeys#NUMERIC_CONFIG}
 * based on the {@link ConfigurationKeys#NUMERIC_CONFIG_MAP} set in the
 * {@link QueryConfigHandler}.
 * 
 * @see NumericConfig
 * @see QueryConfigHandler
 * @see ConfigurationKeys#NUMERIC_CONFIG
 * @see ConfigurationKeys#NUMERIC_CONFIG_MAP
 */
public class NumericFieldConfigListener implements FieldConfigListener {
  
  final private QueryConfigHandler config;
  
  /**
   * Construcs a {@link NumericFieldConfigListener} object using the given {@link QueryConfigHandler}.
   * 
   * @param config the {@link QueryConfigHandler} it will listen too
   */
  public NumericFieldConfigListener(QueryConfigHandler config) {
    
    if (config == null) {
      throw new IllegalArgumentException("config cannot be null!");
    }
    
    this.config = config;
    
  }
  
  @Override
  public void buildFieldConfig(FieldConfig fieldConfig) {
    Map<String,NumericConfig> numericConfigMap = config
        .get(ConfigurationKeys.NUMERIC_CONFIG_MAP);
    
    if (numericConfigMap != null) {
      NumericConfig numericConfig = numericConfigMap
          .get(fieldConfig.getField());
      
      if (numericConfig != null) {
        fieldConfig.set(ConfigurationKeys.NUMERIC_CONFIG, numericConfig);
      }
      
    }
    
  }
  
}
