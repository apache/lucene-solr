package org.apache.lucene.store.instantiated;

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;

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

/**
 * Essetially a Map<FieldName, {@link org.apache.lucene.store.instantiated.FieldSetting}> 
 */
class FieldSettings {


  FieldSettings() {
  }

  private Map</** field name */String, FieldSetting> fieldSettings = new HashMap<String, FieldSetting>();

  synchronized FieldSetting merge(FieldSetting fieldSetting) {
    FieldSetting setting = fieldSettings.get(fieldSetting.fieldName);

    if (setting == null) {
      setting = new FieldSetting(fieldSetting.fieldName);
      fieldSettings.put(fieldSetting.fieldName, setting);
    }

    if (fieldSetting.stored) {
      setting.stored = true;
    }
    if (fieldSetting.compressed) {
      setting.compressed = true;
    }

    if ("b3".equals(fieldSetting.fieldName)) {
      System.currentTimeMillis();
    }
    if (fieldSetting.indexed) {
      setting.indexed = true;
    }
    if (fieldSetting.tokenized) {
      setting.tokenized = true;
    }

    if (fieldSetting.storeTermVector) {
      setting.storeTermVector = true;
    }
    if (fieldSetting.storeOffsetWithTermVector) {
      setting.storeOffsetWithTermVector = true;
    }
    if (fieldSetting.storePositionWithTermVector) {
      setting.storePositionWithTermVector = true;
    }

    if (fieldSetting.storePayloads) {
      setting.storePayloads = true;
    }

    return setting;

  }

  FieldSetting get(String name) {
    return fieldSettings.get(name);
  }

  FieldSetting get(String name, boolean create) {
    FieldSetting fieldSetting = fieldSettings.get(name);
    if (create && fieldSetting == null) {
      fieldSetting = new FieldSetting(name);
      fieldSettings.put(name, fieldSetting);
    }
    return fieldSetting;
  }

  Collection<FieldSetting> values() {
    return fieldSettings.values();
  }

}
