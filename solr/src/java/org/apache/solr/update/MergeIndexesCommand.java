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

import org.apache.lucene.store.Directory;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A merge indexes command encapsulated in an object.
 *
 * @since solr 1.4
 *
 */
public class MergeIndexesCommand extends UpdateCommand {
  public Directory[] dirs;

  public MergeIndexesCommand(SolrQueryRequest req) {
    this(null, req);
  }

  public MergeIndexesCommand(Directory[] dirs, SolrQueryRequest req) {
    super("mergeIndexes", req);
    this.dirs = dirs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(commandName);
    sb.append(':');
    if (dirs != null && dirs.length > 0) {
      sb.append(dirs[0]);
      for (int i = 1; i < dirs.length; i++) {
        sb.append(",").append(dirs[i]);
      }
    }
    return sb.toString();
  }
}
