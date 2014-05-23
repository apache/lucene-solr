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
package org.apache.solr.hadoop;

public enum SolrCounters {

  DOCUMENTS_WRITTEN (getClassName(SolrReducer.class) 
      + ": Number of documents processed"),

  BATCHES_WRITTEN (getClassName(SolrReducer.class) 
      + ": Number of document batches processed"),

  BATCH_WRITE_TIME (getClassName(SolrReducer.class) 
      + ": Time spent by reducers writing batches [ms]"),

  PHYSICAL_REDUCER_MERGE_TIME (getClassName(SolrReducer.class)
      + ": Time spent by reducers on physical merges [ms]"),
  
  LOGICAL_TREE_MERGE_TIME (getClassName(TreeMergeMapper.class)
      + ": Time spent on logical tree merges [ms]"),

  PHYSICAL_TREE_MERGE_TIME (getClassName(TreeMergeMapper.class)
      + ": Time spent on physical tree merges [ms]");

  private final String label;
  
  private SolrCounters(String label) {
    this.label = label;
  }
  
  public String toString() {
    return label;
  }
  
  private static String getClassName(Class clazz) {
    return Utils.getShortClassName(clazz);
  }

}
