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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.HdfsFileFieldNames;
import org.apache.solr.hadoop.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UpdateConflictResolver implementation that ignores all but the most recent
 * document version, based on a configurable numeric Solr field, which defaults
 * to the file_last_modified timestamp.
 */
public class RetainMostRecentUpdateConflictResolver implements UpdateConflictResolver, Configurable {

  private Configuration conf;
  private String orderByFieldName = ORDER_BY_FIELD_NAME_DEFAULT;
  
  public static final String ORDER_BY_FIELD_NAME_KEY = 
      RetainMostRecentUpdateConflictResolver.class.getName() + ".orderByFieldName";
  
  public static final String ORDER_BY_FIELD_NAME_DEFAULT = HdfsFileFieldNames.FILE_LAST_MODIFIED;

  public static final String COUNTER_GROUP = Utils.getShortClassName(RetainMostRecentUpdateConflictResolver.class);
  public static final String DUPLICATES_COUNTER_NAME = "Number of documents ignored as duplicates";
  public static final String OUTDATED_COUNTER_NAME =   "Number of documents ignored as outdated";
  
  private static final Logger LOG = LoggerFactory.getLogger(RetainMostRecentUpdateConflictResolver.class);
  
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
    return getMaximum(updates, getOrderByFieldName(), new SolrInputDocumentComparator.TimeStampComparator(), ctx);
  }

  /** Returns the most recent document among the colliding updates */
  protected Iterator<SolrInputDocument> getMaximum(Iterator<SolrInputDocument> updates, String fieldName,
      Comparator child, Context context) {
    
    SolrInputDocumentComparator comp = new SolrInputDocumentComparator(fieldName, child);
    SolrInputDocument max = null;
    long numDupes = 0;
    long numOutdated = 0;
    while (updates.hasNext()) {
      SolrInputDocument next = updates.next(); 
      assert next != null;
      if (max == null) {
        max = next;
      } else {
        int c = comp.compare(next, max);
        if (c == 0) {
          LOG.debug("Ignoring document version because it is a duplicate: {}", next);
          numDupes++;
        } else if (c > 0) {
          LOG.debug("Ignoring document version because it is outdated: {}", max);
          max = next;
          numOutdated++;
        } else {
          LOG.debug("Ignoring document version because it is outdated: {}", next);        
          numOutdated++;
        }
      }
    }
    
    assert max != null;
    if (numDupes > 0) {
      context.getCounter(COUNTER_GROUP, DUPLICATES_COUNTER_NAME).increment(numDupes);
    }
    if (numOutdated > 0) {
      context.getCounter(COUNTER_GROUP, OUTDATED_COUNTER_NAME).increment(numOutdated);
    }
    return Collections.singletonList(max).iterator();
  }
    
}
