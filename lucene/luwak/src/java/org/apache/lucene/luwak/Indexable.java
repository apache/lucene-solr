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

package org.apache.lucene.luwak;

import org.apache.lucene.document.Document;

/**
 * An indexable query to be added to the Monitor's queryindex
 */
public class Indexable {

  /**
   * The id of the parent {@link MonitorQuery}
   */
  public final String id;

  /**
   * The {@link QueryCacheEntry} to be indexed
   */
  public final QueryCacheEntry queryCacheEntry;

  /**
   * A representation of the {@link QueryCacheEntry} as a lucene {@link Document}
   */
  public final Document document;

  public Indexable(String id, QueryCacheEntry queryCacheEntry, Document document) {
    this.id = id;
    this.queryCacheEntry = queryCacheEntry;
    this.document = document;
  }
}
