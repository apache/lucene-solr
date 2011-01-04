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
package org.apache.lucene.benchmark.quality.utils;

import java.io.IOException;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.search.Searcher;

/**
 * Utility: extract doc names from an index
 */
public class DocNameExtractor {

  private FieldSelector fldSel;
  private String docNameField;
  
  /**
   * Constructor for DocNameExtractor.
   * @param docNameField name of the stored field containing the doc name. 
   */
  public DocNameExtractor (final String docNameField) {
    this.docNameField = docNameField;
    fldSel = new FieldSelector() {
      public FieldSelectorResult accept(String fieldName) {
        return fieldName.equals(docNameField) ? 
            FieldSelectorResult.LOAD_AND_BREAK :
              FieldSelectorResult.NO_LOAD;
      }
    };
  }
  
  /**
   * Extract the name of the input doc from the index.
   * @param searcher access to the index.
   * @param docid ID of doc whose name is needed.
   * @return the name of the input doc as extracted from the index.
   * @throws IOException if cannot extract the doc name from the index.
   */
  public String docName(Searcher searcher, int docid) throws IOException {
    return searcher.doc(docid,fldSel).get(docNameField);
  }
  
}
