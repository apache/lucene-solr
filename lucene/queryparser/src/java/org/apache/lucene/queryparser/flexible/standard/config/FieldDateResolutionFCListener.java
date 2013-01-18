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

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfigListener;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;

/**
 * This listener listens for every field configuration request and assign a
 * {@link ConfigurationKeys#DATE_RESOLUTION} to the equivalent {@link FieldConfig} based
 * on a defined map: fieldName -> {@link Resolution} stored in
 * {@link ConfigurationKeys#FIELD_DATE_RESOLUTION_MAP}.
 * 
 * @see ConfigurationKeys#DATE_RESOLUTION
 * @see ConfigurationKeys#FIELD_DATE_RESOLUTION_MAP
 * @see FieldConfig
 * @see FieldConfigListener
 */
public class FieldDateResolutionFCListener implements FieldConfigListener {

  private QueryConfigHandler config = null;

  public FieldDateResolutionFCListener(QueryConfigHandler config) {
    this.config = config;
  }

  @Override
  public void buildFieldConfig(FieldConfig fieldConfig) {
    DateTools.Resolution dateRes = null;
    Map<CharSequence, DateTools.Resolution> dateResMap = this.config.get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP);

    if (dateResMap != null) {
      dateRes = dateResMap.get(
          fieldConfig.getField());
    }

    if (dateRes == null) {
      dateRes = this.config.get(ConfigurationKeys.DATE_RESOLUTION);
    }

    if (dateRes != null) {
      fieldConfig.set(ConfigurationKeys.DATE_RESOLUTION, dateRes);
    }

  }

}
