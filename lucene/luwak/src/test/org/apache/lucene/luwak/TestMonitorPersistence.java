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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.presearcher.TermFilteredPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestMonitorPersistence extends LuceneTestCase {

  private Path indexDirectory = createTempDir();

  public void testCacheIsRepopulated() throws IOException, UpdateException {

    InputDocument doc = InputDocument.builder("doc1").addField("f", "test", new StandardAnalyzer()).build();

    try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher(),
        new MMapDirectory(indexDirectory))) {
      monitor.update(new MonitorQuery("1", "test"),
          new MonitorQuery("2", "test"),
          new MonitorQuery("3", "test", Collections.singletonMap("language", "en")),
          new MonitorQuery("4", "test", Collections.singletonMap("wibble", "quack")));

      assertEquals(4, monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount("doc1"));
    }

    try (Monitor monitor2 = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher(),
        new MMapDirectory(indexDirectory))) {

      assertEquals(4, monitor2.getQueryCount());
      assertEquals(4, monitor2.match(doc, SimpleMatcher.FACTORY).getMatchCount("doc1"));
    }

  }

  public void testMonitorCanAvoidStoringQueries() throws IOException, UpdateException {

    QueryIndexConfiguration config = new QueryIndexConfiguration().storeQueries(false);
    InputDocument doc = InputDocument.builder("doc1").addField("f", "test", new StandardAnalyzer()).build();

    try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher(),
        new MMapDirectory(indexDirectory), config)) {

      monitor.update(new MonitorQuery("1", "test"),
          new MonitorQuery("2", "test"),
          new MonitorQuery("3", "test", Collections.singletonMap("language", "en")),
          new MonitorQuery("4", "test", Collections.singletonMap("wibble", "quack")));

      assertEquals(4, monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount("doc1"));
    }

    try (Monitor monitor2 = new Monitor(new LuceneQueryParser("f"), new TermFilteredPresearcher(),
        new MMapDirectory(indexDirectory), config)) {

      assertEquals(0, monitor2.getQueryCount());
      assertEquals(0, monitor2.getDisjunctCount());
      IllegalStateException e = expectThrows(IllegalStateException.class, () -> monitor2.getQuery("query"));
      assertEquals("Cannot call getQuery() as queries are not stored", e.getMessage());

    }

  }

}
