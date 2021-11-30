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

package org.apache.solr.handler.designer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.solr.schema.ManagedIndexSchema;

class SchemaDesignerSettings implements SchemaDesignerConstants {

  private String copyFrom;
  private boolean isDisabled;
  private List<String> languages;
  private boolean dynamicFieldsEnabled;
  private boolean nestedDocsEnabled;
  private boolean fieldGuessingEnabled;
  private Integer publishedVersion;
  private ManagedIndexSchema schema;

  @SuppressWarnings("unchecked")
  SchemaDesignerSettings(Map<String, Object> stored) {
    this.isDisabled = getSettingAsBool(stored, DESIGNER_KEY + DISABLED, false);
    this.publishedVersion = null;
    this.copyFrom = (String) stored.get(DESIGNER_KEY + COPY_FROM_PARAM);
    this.languages = (List<String>) stored.getOrDefault(DESIGNER_KEY + LANGUAGES_PARAM, Collections.emptyList());
    this.dynamicFieldsEnabled = getSettingAsBool(stored, DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, true);
    this.nestedDocsEnabled = getSettingAsBool(stored, DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, false);
    this.fieldGuessingEnabled = getSettingAsBool(stored, AUTO_CREATE_FIELDS, true);
  }

  static boolean getSettingAsBool(final Map<String, Object> stored, final String key, final boolean defaultValue) {
    boolean settingAsBool = defaultValue;
    final Object settingValue = stored != null ? stored.get(key) : null;
    if (settingValue != null) {
      // covers either a Boolean or String object in the map
      settingAsBool = Boolean.parseBoolean(settingValue.toString());
    }
    return settingAsBool;
  }

  Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(DESIGNER_KEY + DISABLED, isDisabled);
    map.put(DESIGNER_KEY + LANGUAGES_PARAM, languages);
    map.put(DESIGNER_KEY + ENABLE_DYNAMIC_FIELDS_PARAM, dynamicFieldsEnabled);
    map.put(DESIGNER_KEY + ENABLE_NESTED_DOCS_PARAM, nestedDocsEnabled);
    map.put(AUTO_CREATE_FIELDS, fieldGuessingEnabled);
    if (copyFrom != null) {
      map.put(DESIGNER_KEY + COPY_FROM_PARAM, copyFrom);
    }
    if (publishedVersion != null) {
      map.put(DESIGNER_KEY + PUBLISHED_VERSION, publishedVersion);
    }
    return map;
  }

  public ManagedIndexSchema getSchema() {
    return schema;
  }

  public void setSchema(ManagedIndexSchema schema) {
    this.schema = schema;
  }

  public Optional<Integer> getPublishedVersion() {
    return Optional.ofNullable(publishedVersion);
  }

  public void setPublishedVersion(int publishedVersion) {
    this.publishedVersion = publishedVersion;
  }

  public String getCopyFrom() {
    return copyFrom;
  }

  public void setCopyFrom(String copyFrom) {
    this.copyFrom = copyFrom;
  }

  public boolean isDisabled() {
    return isDisabled;
  }

  public void setDisabled(boolean isDisabled) {
    this.isDisabled = isDisabled;
  }

  public List<String> getLanguages() {
    return languages;
  }

  public void setLanguages(List<String> langs) {
    this.languages = langs != null ? langs : Collections.emptyList();
  }

  public boolean dynamicFieldsEnabled() {
    return dynamicFieldsEnabled;
  }

  public void setDynamicFieldsEnabled(boolean enabled) {
    this.dynamicFieldsEnabled = enabled;
  }

  public boolean nestedDocsEnabled() {
    return nestedDocsEnabled;
  }

  public void setNestedDocsEnabled(boolean enabled) {
    this.nestedDocsEnabled = enabled;
  }

  public boolean fieldGuessingEnabled() {
    return fieldGuessingEnabled;
  }

  public void setFieldGuessingEnabled(boolean enabled) {
    this.fieldGuessingEnabled = enabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaDesignerSettings that = (SchemaDesignerSettings) o;
    return isDisabled == that.isDisabled &&
        dynamicFieldsEnabled == that.dynamicFieldsEnabled &&
        nestedDocsEnabled == that.nestedDocsEnabled &&
        fieldGuessingEnabled == that.fieldGuessingEnabled &&
        Objects.equals(copyFrom, that.copyFrom) &&
        languages.equals(that.languages) &&
        Objects.equals(publishedVersion, that.publishedVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(copyFrom, isDisabled, languages, dynamicFieldsEnabled, nestedDocsEnabled, fieldGuessingEnabled, publishedVersion);
  }
}
