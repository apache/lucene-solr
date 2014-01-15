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
package org.apache.solr.hadoop.dedup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.HdfsFileFieldNames;

/**
 * UpdateConflictResolver implementation that orders colliding updates ascending
 * from least recent to most recent (partial) update, based on a configurable
 * numeric Solr field, which defaults to the file_last_modified timestamp.
 */
public class SortingUpdateConflictResolver implements UpdateConflictResolver, Configurable {

  private Configuration conf;
  private String orderByFieldName = ORDER_BY_FIELD_NAME_DEFAULT;
  
  public static final String ORDER_BY_FIELD_NAME_KEY = 
      SortingUpdateConflictResolver.class.getName() + ".orderByFieldName";
  
  public static final String ORDER_BY_FIELD_NAME_DEFAULT = HdfsFileFieldNames.FILE_LAST_MODIFIED;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.orderByFieldName = conf.get(ORDER_BY_FIELD_NAME_KEY, orderByFieldName);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  protected String getOrderByFieldName() {
    return orderByFieldName;
  }
  
  @Override
  public Iterator<SolrInputDocument> orderUpdates(Text key, Iterator<SolrInputDocument> updates, Context ctx) {    
    return sort(updates, getOrderByFieldName(), new SolrInputDocumentComparator.TimeStampComparator());
  }

  protected Iterator<SolrInputDocument> sort(Iterator<SolrInputDocument> updates, String fieldName, Comparator child) {
    // TODO: use an external merge sort in the pathological case where there are a huge amount of collisions
    List<SolrInputDocument> sortedUpdates = new ArrayList(1); 
    while (updates.hasNext()) {
      sortedUpdates.add(updates.next());
    }
    if (sortedUpdates.size() > 1) { // conflicts are rare
      Collections.sort(sortedUpdates, new SolrInputDocumentComparator(fieldName, child));
    }
    return sortedUpdates.iterator();
  }
    
}
