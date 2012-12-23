package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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

/**
 * {@link Fields} of a segment with updates.
 */
public class StackedFields extends Fields {
  
  final Map<String,Terms> fields;
  
  public StackedFields(Fields[] fieldsArray,
      Map<String,FieldGenerationReplacements> replacementsMap, int doc)
      throws IOException {
    fields = new TreeMap<String,Terms>();
    final Set<String> ignoreFields = new HashSet<String>();
    
    for (int i = fieldsArray.length - 1; i >= 0; i--) {
      if (fieldsArray[i] != null) {
        final Iterator<String> iterator = fieldsArray[i].iterator();
        while (iterator.hasNext()) {
          // handle single field
          String field = iterator.next();
          if (!ignoreFields.contains(field)) {
            Terms terms = fieldsArray[i].terms(field);
            if (terms != null) {
              StackedTerms stackedTerms = (StackedTerms) fields.get(field);
              if (stackedTerms == null) {
                stackedTerms = new StackedTerms(fieldsArray.length,
                    replacementsMap.get(field));
                fields.put(field, stackedTerms);
              }
              stackedTerms.addTerms(terms, i);
            }
          }
        }
      }
      
      if (doc >= 0) {
        // ignore fields according to replacements for this document
        for (Entry<String,FieldGenerationReplacements> entry : replacementsMap
            .entrySet()) {
          if (!ignoreFields.contains(entry.getKey())
              && entry.getValue() != null && entry.getValue().get(doc) == i) {
            ignoreFields.add(entry.getKey());
          }
        }
      }
    }
  }
  
  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }
  
  @Override
  public Terms terms(String field) throws IOException {
    return fields.get(field);
  }
  
  @Override
  public int size() {
    return fields.size();
  }
  
}
