package org.apache.lucene.benchmark.byTask.tasks;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.IndexReader;

/**
 * Search and Traverse and Retrieve docs task using a
 * FieldVisitor loading only the requested fields.
 *
 * <p>Note: This task reuses the reader if it is already open.
 * Otherwise a reader is opened at start and closed at the end.
 *
 * <p>Takes optional param: comma separated list of Fields to load.</p>
 * 
 * <p>Other side effects: counts additional 1 (record) for each traversed hit, 
 * and 1 more for each retrieved (non null) document.</p>
 */
public class SearchTravRetLoadFieldSelectorTask extends SearchTravTask {

  protected Set<String> fieldsToLoad;

  public SearchTravRetLoadFieldSelectorTask(PerfRunData runData) {
    super(runData);
    
  }

  @Override
  public boolean withRetrieve() {
    return true;
  }


  @Override
  protected Document retrieveDoc(IndexReader ir, int id) throws IOException {
    if (fieldsToLoad == null) {
      return ir.document(id);
    } else {
      DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(fieldsToLoad);
      ir.document(id, visitor);
      return visitor.getDocument();
    }
  }

  @Override
  public void setParams(String params) {
    this.params = params; // cannot just call super.setParams(), b/c it's params differ.
    fieldsToLoad = new HashSet<String>();
    for (StringTokenizer tokenizer = new StringTokenizer(params, ","); tokenizer.hasMoreTokens();) {
      String s = tokenizer.nextToken();
      fieldsToLoad.add(s);
    }
  }


  /* (non-Javadoc)
  * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
  */
  @Override
  public boolean supportsParams() {
    return true;
  }
}
