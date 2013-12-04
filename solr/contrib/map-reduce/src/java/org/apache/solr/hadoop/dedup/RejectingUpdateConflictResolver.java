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
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.solr.common.SolrInputDocument;

/**
 * UpdateConflictResolver implementation that rejects multiple documents with
 * the same key with an exception.
 */
public final class RejectingUpdateConflictResolver implements UpdateConflictResolver {

  @Override
  public Iterator<SolrInputDocument> orderUpdates(Text key, Iterator<SolrInputDocument> updates, Context ctx) {    
    SolrInputDocument firstUpdate = null;
    while (updates.hasNext()) {
      if (firstUpdate == null) {
        firstUpdate = updates.next();
        assert firstUpdate != null;
      } else {
        throw new IllegalArgumentException("Update conflict! Documents with the same unique key are forbidden: "
            + key);
      }
    }
    assert firstUpdate != null;
    return Collections.singletonList(firstUpdate).iterator();
  }

}
