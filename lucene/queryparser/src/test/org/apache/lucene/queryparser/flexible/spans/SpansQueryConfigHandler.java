package org.apache.lucene.queryparser.flexible.spans;

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

import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;

/**
 * This query config handler only adds the {@link UniqueFieldAttribute} to it.<br/>
 * <br/>
 * 
 * It does not return any configuration for a field in specific.
 */
public class SpansQueryConfigHandler extends QueryConfigHandler {
  
  final public static ConfigurationKey<String> UNIQUE_FIELD = ConfigurationKey.newInstance();
  
  public SpansQueryConfigHandler() {
    // empty constructor
  }

  @Override
  public FieldConfig getFieldConfig(String fieldName) {

    // there is no field configuration, always return null
    return null;

  }

}
