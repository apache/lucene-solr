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


import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.io.IOException;

/**
 * Search and Traverse and Retrieve docs task using a SetBasedFieldSelector.
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

  protected FieldSelector fieldSelector;
  public SearchTravRetLoadFieldSelectorTask(PerfRunData runData) {
    super(runData);
    
  }

  public boolean withRetrieve() {
    return true;
  }


  protected Document retrieveDoc(IndexReader ir, int id) throws IOException {
    return ir.document(id, fieldSelector);
  }

  public void setParams(String params) {
    this.params = params; // cannot just call super.setParams(), b/c it's params differ.
    Set fieldsToLoad = new HashSet();
    for (StringTokenizer tokenizer = new StringTokenizer(params, ","); tokenizer.hasMoreTokens();) {
      String s = tokenizer.nextToken();
      fieldsToLoad.add(s);
    }
    fieldSelector = new SetBasedFieldSelector(fieldsToLoad, Collections.EMPTY_SET);
  }


  /* (non-Javadoc)
  * @see org.apache.lucene.benchmark.byTask.tasks.PerfTask#supportsParams()
  */
  public boolean supportsParams() {
    return true;
  }
}
