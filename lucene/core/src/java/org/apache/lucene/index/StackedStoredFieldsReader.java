package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.codecs.StoredFieldsReader;

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

public class StackedStoredFieldsReader extends StoredFieldsReader {
  
  private StoredFieldsReader[] allReaders;
  private Map<String,FieldGenerationReplacements> replacementsMap;
  
  public StackedStoredFieldsReader(StoredFieldsReader[] allReaders,
      Map<String,FieldGenerationReplacements> replacements) {
    this.allReaders = allReaders;
    this.replacementsMap = replacements;
  }
  
  @Override
  public void close() throws IOException {
    for (StoredFieldsReader reader : allReaders) {
      reader.close();
    }
  }
  
  @Override
  public void visitDocument(int n, StoredFieldVisitor visitor,
      Set<String> ignoreFields) throws IOException {
    ignoreFields = new HashSet<String>();
    // go over stacked segments from top to bottom
    for (int i = allReaders.length - 1; i > 0; i--) {
      // visit current stacked segment 
      allReaders[i].visitDocument(n, visitor, ignoreFields);
      // accumulate fields to ignore in lower stacked segments
      for (Entry<String,FieldGenerationReplacements> entry : replacementsMap
          .entrySet()) {
        if (!ignoreFields.contains(entry.getKey()) && entry.getValue() != null
            && entry.getValue().get(n) == i) {
          ignoreFields.add(entry.getKey());
        }
      }
    }
    // now visit core
    allReaders[0].visitDocument(n, visitor, ignoreFields);
  }
  
  @Override
  public StoredFieldsReader clone() {
    StoredFieldsReader[] newReaders = new StoredFieldsReader[allReaders.length];
    for (int i = 0; i < newReaders.length; i++) {
      newReaders[i] = allReaders[i].clone();
    }
    return new StackedStoredFieldsReader(newReaders, replacementsMap);
  }
  
}
