package org.apache.lucene.index;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.codecs.TermVectorsReader;

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

public class StackedTermVectorsReader extends TermVectorsReader {
  
  private final TermVectorsReader[] allReaders;
  private final Map<String,FieldGenerationReplacements> replacementsMap;
  private final int docId;
  
  public StackedTermVectorsReader(TermVectorsReader[] allReaders,
      Map<String,FieldGenerationReplacements> replacementsMap, int docId) {
    this.allReaders = allReaders;
    this.replacementsMap = replacementsMap;
    this.docId = docId;
  }
  
  @Override
  public void close() throws IOException {}
  
  @Override
  public Fields get(int doc) throws IOException {
    // in case docId != -1, we need to create special fields, where document
    // docId is returned as doc 0, and all others ignored
    if (docId != -1) {
      if (doc != 0) {
        return null;
      }
    }
    // generate fields array
    Fields[] fieldsArray = new Fields[allReaders.length];
    for (int i = 0; i < fieldsArray.length; i++) {
      if (allReaders[i] != null) {
        fieldsArray[i] = allReaders[i].get(doc);
      }
    }
    return new StackedFields(fieldsArray, replacementsMap, doc);
  }
  
  @Override
  public TermVectorsReader clone() {
    return new StackedTermVectorsReader(allReaders, replacementsMap, docId);
  }
}
