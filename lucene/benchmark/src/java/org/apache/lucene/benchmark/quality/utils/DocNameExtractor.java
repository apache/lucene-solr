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
package org.apache.lucene.benchmark.quality.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;

/**
 * Utility: extract doc names from an index
 */
public class DocNameExtractor {

  private final String docNameField;
  
  /**
   * Constructor for DocNameExtractor.
   * @param docNameField name of the stored field containing the doc name. 
   */
  public DocNameExtractor (final String docNameField) {
    this.docNameField = docNameField;
  }
  
  /**
   * Extract the name of the input doc from the index.
   * @param searcher access to the index.
   * @param docid ID of doc whose name is needed.
   * @return the name of the input doc as extracted from the index.
   * @throws IOException if cannot extract the doc name from the index.
   */
  public String docName(IndexSearcher searcher, int docid) throws IOException {
    final List<String> name = new ArrayList<>();
    searcher.getIndexReader().document(docid, new StoredFieldVisitor() {
        @Override
        public void stringField(FieldInfo fieldInfo, byte[] bytes) {
          String value = new String(bytes, StandardCharsets.UTF_8);
          name.add(value);
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
          if (!name.isEmpty()) {
            return Status.STOP;
          } else if (fieldInfo.name.equals(docNameField)) {
            return Status.YES;
          } else {
            return Status.NO;
          }
        }
      });
    if (name.size() != 0) {
      return name.get(0);
    } else {
      return null;
    }
  }
  
}
