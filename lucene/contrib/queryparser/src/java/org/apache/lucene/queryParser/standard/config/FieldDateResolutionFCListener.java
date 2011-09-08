package org.apache.lucene.queryParser.standard.config;

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

import org.apache.lucene.document.DateTools;
import org.apache.lucene.queryParser.core.config.FieldConfig;
import org.apache.lucene.queryParser.core.config.FieldConfigListener;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

/**
 * This listener listens for every field configuration request and assign a
 * {@link DateResolutionAttribute} to the equivalent {@link FieldConfig} based
 * on a defined map: fieldName -> DateTools.Resolution stored in
 * {@link FieldDateResolutionMapAttribute} in the
 * {@link DateResolutionAttribute}.
 * 
 * @see DateResolutionAttribute
 * @see FieldDateResolutionMapAttribute
 * @see FieldConfig
 * @see FieldConfigListener
 */
public class FieldDateResolutionFCListener implements FieldConfigListener {

  private static final long serialVersionUID = -5929802948798314067L;

  private QueryConfigHandler config = null;

  public FieldDateResolutionFCListener(QueryConfigHandler config) {
    this.config = config;
  }

  public void buildFieldConfig(FieldConfig fieldConfig) {
    DateTools.Resolution dateRes = null;

    if (this.config.has(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP)) {
      dateRes = this.config.get(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP).get(
          fieldConfig.getField());
    }

    if (dateRes == null) {
      dateRes = this.config.get(ConfigurationKeys.DATE_RESOLUTION);
    }

    fieldConfig.addAttribute(DateResolutionAttribute.class).setDateResolution(dateRes);
    
    // uncomment code below when deprecated query parser attributes are removed
    // fieldConfig.set(ConfigurationKeys.DATE_RESOLUTION, dateRes);

  }

}
