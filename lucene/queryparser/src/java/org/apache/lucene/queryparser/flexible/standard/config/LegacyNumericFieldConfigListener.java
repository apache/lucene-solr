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
package org.apache.lucene.queryparser.flexible.standard.config;

import java.util.Map;

import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfigListener;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

/**
 * This listener is used to listen to {@link FieldConfig} requests in
 * {@link QueryConfigHandler} and add {@link ConfigurationKeys#LEGACY_NUMERIC_CONFIG}
 * based on the {@link ConfigurationKeys#LEGACY_NUMERIC_CONFIG_MAP} set in the
 * {@link QueryConfigHandler}.
 * 
 * @see LegacyNumericConfig
 * @see QueryConfigHandler
 * @see ConfigurationKeys#LEGACY_NUMERIC_CONFIG
 * @see ConfigurationKeys#LEGACY_NUMERIC_CONFIG_MAP
 * @deprecated Index with Points instead and use {@link PointsConfigListener}
 */
@Deprecated
public class LegacyNumericFieldConfigListener implements FieldConfigListener {
  
  final private QueryConfigHandler config;
  
  /**
   * Constructs a {@link LegacyNumericFieldConfigListener} object using the given {@link QueryConfigHandler}.
   * 
   * @param config the {@link QueryConfigHandler} it will listen too
   */
  public LegacyNumericFieldConfigListener(QueryConfigHandler config) {
    
    if (config == null) {
      throw new IllegalArgumentException("config must not be null!");
    }
    
    this.config = config;
    
  }
  
  @Override
  public void buildFieldConfig(FieldConfig fieldConfig) {
    Map<String,LegacyNumericConfig> numericConfigMap = config
        .get(ConfigurationKeys.LEGACY_NUMERIC_CONFIG_MAP);
    
    if (numericConfigMap != null) {
      LegacyNumericConfig numericConfig = numericConfigMap
          .get(fieldConfig.getField());
      
      if (numericConfig != null) {
        fieldConfig.set(ConfigurationKeys.LEGACY_NUMERIC_CONFIG, numericConfig);
      }
      
    }
    
  }
  
}
