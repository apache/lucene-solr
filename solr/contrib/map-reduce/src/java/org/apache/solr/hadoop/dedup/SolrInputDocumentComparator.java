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

import java.util.Comparator;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

/**
 * Default mechanism of determining which of two Solr documents with the same
 * key is the more recent version.
 */
public final class SolrInputDocumentComparator implements Comparator<SolrInputDocument> {
  
  private Comparator child;
  private String fieldName;

  SolrInputDocumentComparator(String fieldName, Comparator child) {
    this.child = child;
    this.fieldName = fieldName;
  }

  @Override
  public int compare(SolrInputDocument doc1, SolrInputDocument doc2) {
    SolrInputField f1 = doc1.getField(fieldName);
    SolrInputField f2 = doc2.getField(fieldName);
    if (f1 == f2) {
      return 0;
    } else if (f1 == null) {
      return -1;
    } else if (f2 == null) {
      return 1;
    }
    
    Object v1 = f1.getFirstValue();
    Object v2 = f2.getFirstValue();          
    return child.compare(v1, v2);
  }
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class TimeStampComparator implements Comparator {

    @Override
    public int compare(Object v1, Object v2) {
      if (v1 == v2) {
        return 0;
      } else if (v1 == null) {
        return -1;
      } else if (v2 == null) {
        return 1;
      }
      long t1 = getLong(v1);
      long t2 = getLong(v2);          
      return (t1 < t2 ? -1 : (t1==t2 ? 0 : 1));
    }
    
    private long getLong(Object v) {
      if (v instanceof Long) {
        return ((Long) v).longValue();
      } else {
        return Long.parseLong(v.toString());
      }
    }      
    
  }

}
