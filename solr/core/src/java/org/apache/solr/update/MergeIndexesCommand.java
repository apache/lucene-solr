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

package org.apache.solr.update;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A merge indexes command encapsulated in an object.
 *
 * @since solr 1.4
 *
 */
public class MergeIndexesCommand extends UpdateCommand {
  public IndexReader[] readers;

  public MergeIndexesCommand(IndexReader[] readers, SolrQueryRequest req) {
    super("mergeIndexes", req);
    this.readers = readers;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(commandName);
    sb.append(':');
    if (readers != null && readers.length > 0) {
      sb.append(readers[0].directory());
      for (int i = 1; i < readers.length; i++) {
        sb.append(",").append(readers[i].directory());
      }
    }
    return sb.toString();
  }
}
