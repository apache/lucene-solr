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
package org.apache.solr.update;

import org.apache.lucene.index.DirectoryReader;
import org.apache.solr.request.SolrQueryRequest;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A merge indexes command encapsulated in an object.
 *
 * @since solr 1.4
 *
 */
public class MergeIndexesCommand extends UpdateCommand {
  public List<DirectoryReader> readers;

  public MergeIndexesCommand(List<DirectoryReader> readers, SolrQueryRequest req) {
    super(req);
    this.readers = readers;
  }

  @Override
  public String name() {
    return "mergeIndexes";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(readers.stream().map(reader-> reader.directory().toString()).collect(Collectors.joining(",")));
    sb.append('}');
    return sb.toString();
  }
}
