package org.apache.solr.rest;

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

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.restlet.resource.ResourceException;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Base class for the FieldType resource classes.
 */
abstract class BaseFieldTypeResource extends BaseSchemaResource {
  private static final String FIELDS = "fields";
  private static final String DYNAMIC_FIELDS = "dynamicFields";

  private boolean showDefaults;

  protected BaseFieldTypeResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
    showDefaults = getSolrRequest().getParams().getBool(SHOW_DEFAULTS, false);
  }
  
  /** Used by subclasses to collect field type properties */
  protected SimpleOrderedMap<Object> getFieldTypeProperties(FieldType fieldType) {
    SimpleOrderedMap<Object> properties = fieldType.getNamedPropertyValues(showDefaults);
    properties.add(FIELDS, getFieldsWithFieldType(fieldType));
    properties.add(DYNAMIC_FIELDS, getDynamicFieldsWithFieldType(fieldType));
    return properties;
  }

  
  /** Return a list of names of Fields that have the given FieldType */
  protected abstract List<String> getFieldsWithFieldType(FieldType fieldType);

  /** Return a list of names of DynamicFields that have the given FieldType */
  protected abstract List<String> getDynamicFieldsWithFieldType(FieldType fieldType);
}
