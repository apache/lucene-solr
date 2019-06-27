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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.MatchAllDocsQuery;

public class TestMonitorPersistence extends MonitorTestBase {

  private Path indexDirectory = createTempDir();

  public void testCacheIsRepopulated() throws IOException {

    Document doc = new Document();
    doc.add(newTextField(FIELD, "test", Field.Store.NO));
    MonitorConfiguration config = new MonitorConfiguration()
        .setIndexPath(indexDirectory, MonitorQuerySerializer.fromParser(MonitorTestBase::parse));

    try (Monitor monitor = new Monitor(ANALYZER, config)) {
      monitor.register(
          mq("1", "test"),
          mq("2", "test"),
          mq("3", "test", "language", "en"),
          mq("4", "test", "wibble", "quack"));

      assertEquals(4, monitor.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());

      IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
          () -> monitor.register(new MonitorQuery("5", new MatchAllDocsQuery(), null, Collections.emptyMap())));
      assertEquals("Cannot add a MonitorQuery with a null string representation to a non-ephemeral Monitor", e.getMessage());
    }

    try (Monitor monitor2 = new Monitor(ANALYZER, config)) {
      assertEquals(4, monitor2.getQueryCount());
      assertEquals(4, monitor2.match(doc, QueryMatch.SIMPLE_MATCHER).getMatchCount());

      MonitorQuery mq = monitor2.getQuery("4");
      assertEquals("quack", mq.getMetadata().get("wibble"));
    }

  }

  public void testEphemeralMonitorDoesNotStoreQueries() throws IOException {

    try (Monitor monitor2 = new Monitor(ANALYZER)) {
      IllegalStateException e = expectThrows(IllegalStateException.class, () -> monitor2.getQuery("query"));
      assertEquals("Cannot get queries from an index with no MonitorQuerySerializer", e.getMessage());
    }

  }

}
