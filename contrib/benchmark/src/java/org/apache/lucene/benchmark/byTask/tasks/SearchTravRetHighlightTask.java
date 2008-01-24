package org.apache.lucene.benchmark.byTask.tasks;

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

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.document.Document;

import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import java.util.Collections;

/**
 * Search and Traverse and Retrieve docs task.  Highlight the fields in the retrieved documents.
 *
 * Uses the {@link org.apache.lucene.search.highlight.SimpleHTMLFormatter} for formatting.
 *
 * <p>Note: This task reuses the reader if it is already open.
 * Otherwise a reader is opened at start and closed at the end.
 * </p>
 *
 * <p>Takes optional multivalued, comma separated param string as: size[&lt;traversal size&gt;],highlight[&lt;int&gt;],maxFrags[&lt;int&gt;],mergeContiguous[&lt;boolean&gt;],fields[name1;name2;...]</p>
 * <ul>
 * <li>traversal size - The number of hits to traverse, otherwise all will be traversed</li>
 * <li>highlight - The number of the hits to highlight.  Will always be less than or equal to traversal size.  Default is Integer.MAX_VALUE (i.e. hits.length())</li>
 * <li>maxFrags - The maximum number of fragments to score by the highlighter</li>
 * <li>mergeContiguous - true if contiguous fragments should be merged.</li>
 * <li>fields - The fields to highlight.  If not specified all fields will be highlighted (or at least attempted)</li>
 * </ul>
 * Example:
 * <pre>"SearchHlgtSameRdr" SearchTravRetHighlight(size[10],highlight[10],mergeContiguous[true],maxFrags[3],fields[body]) > : 1000
 * </pre>
 *
 * Documents must be stored in order for this task to work.  Additionally, term vector positions can be used as well.
 *
 * <p>Other side effects: counts additional 1 (record) for each traversed hit,
 * and 1 more for each retrieved (non null) document and 1 for each fragment returned.</p>
 */
public class SearchTravRetHighlightTask extends SearchTravTask {

  protected int numToHighlight = Integer.MAX_VALUE;
  protected boolean mergeContiguous;
  protected int maxFrags = 2;
  protected Set paramFields = Collections.EMPTY_SET;
  

  public SearchTravRetHighlightTask(PerfRunData runData) {
    super(runData);
  }

  public void setup() throws Exception {
    super.setup();
    //check to make sure either the doc is being stored
    PerfRunData data = getRunData();
    if (data.getConfig().get("doc.stored", false) == false){
      throw new Exception("doc.stored must be set to true");
    }
  }

  public boolean withRetrieve() {
    return true;
  }

  public int numToHighlight() {
    return numToHighlight;
  }

  public boolean isMergeContiguousFragments() {
    return mergeContiguous;
  }

  public int maxNumFragments() {
    return maxFrags;
  }

  protected Collection/*<String>*/ getFieldsToHighlight(Document document) {
    Collection result = super.getFieldsToHighlight(document);
    //if stored is false, then result will be empty, in which case just get all the param fields
    if (paramFields.isEmpty() == false && result.isEmpty() == false) {
      result.retainAll(paramFields);
    } else {
      result = paramFields;
    }
    return result;
  }

  public void setParams(String params) {
    String [] splits = params.split(",");
    for (int i = 0; i < splits.length; i++) {
      if (splits[i].startsWith("size[") == true){
        traversalSize = (int)Float.parseFloat(splits[i].substring("size[".length(),splits[i].length() - 1));
      } else if (splits[i].startsWith("highlight[") == true){
        numToHighlight = (int)Float.parseFloat(splits[i].substring("highlight[".length(),splits[i].length() - 1));
      } else if (splits[i].startsWith("maxFrags[") == true){
        maxFrags = (int)Float.parseFloat(splits[i].substring("maxFrags[".length(),splits[i].length() - 1));
      } else if (splits[i].startsWith("mergeContiguous[") == true){
        mergeContiguous = Boolean.valueOf(splits[i].substring("mergeContiguous[".length(),splits[i].length() - 1)).booleanValue();
      } else if (splits[i].startsWith("fields[") == true){
        paramFields = new HashSet();
        String fieldNames = splits[i].substring("fields[".length(), splits[i].length() - 1);
        String [] fieldSplits = fieldNames.split(";");
        for (int j = 0; j < fieldSplits.length; j++) {
          paramFields.add(fieldSplits[j]);          
        }

      }
    }
  }


}