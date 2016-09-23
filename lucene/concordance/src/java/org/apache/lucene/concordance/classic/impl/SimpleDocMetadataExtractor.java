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

package org.apache.lucene.concordance.classic.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.concordance.classic.DocMetadataExtractor;

/**
 * Simple class that returns a map of key value pairs
 * for the fields specified during initialization.
 * <p>
 * Beware! For multi-valued fields, this will take only the first value.
 */
public class SimpleDocMetadataExtractor implements DocMetadataExtractor {

  private Set<String> fields = new HashSet<>();

  public SimpleDocMetadataExtractor(String... fields) {
    for (String f : fields) {
      this.fields.add(f);
    }
  }

  public SimpleDocMetadataExtractor(Set<String> fields) {
    this.fields.addAll(fields);
  }

  public void addField(String f) {
    fields.add(f);
  }

  @Override
  public Set<String> getFieldSelector() {
    return Collections.unmodifiableSet(fields);
  }

  @Override
  public Map<String, String> extract(Document d) {
    Map<String, String> map = new HashMap<>();
    // only takes the first value in a multi-valued field!!!
    for (String fieldName : getFieldSelector()) {
      String[] fieldValues = d.getValues(fieldName);

      if (fieldValues != null && fieldValues.length > 0) {
        map.put(fieldName, fieldValues[0]);
      }
    }
    return map;
  }

}
