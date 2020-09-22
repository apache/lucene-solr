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
package org.apache.solr.cloud.autoscaling.sim;

import java.util.Iterator;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

/**
 * Lightweight generator of fake documents
 * NOTE: this iterator only ever returns the same document N times, which works ok
 * for our "bulk index update" simulation. Obviously don't use this for real indexing.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class FakeDocIterator implements Iterator<SolrInputDocument> {
  final SolrInputDocument doc = new SolrInputDocument();
  final SolrInputField idField = new SolrInputField("id");

  final long start, count;

  long current, max;

  FakeDocIterator(long start, long count) {
    this.start = start;
    this.count = count;
    current = start;
    max = start + count;
    doc.put("id", idField);
    idField.setValue("foo");
  }

  @Override
  public boolean hasNext() {
    return current < max;
  }

  @Override
  public SolrInputDocument next() {
    current++;
    return doc;
  }
}
