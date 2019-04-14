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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.util.LuceneTestCase;

public class TestConcurrentQueryLoader extends LuceneTestCase {

  public void testLoading() throws Exception {

    try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new MatchAllPresearcher())) {
      List<QueryError> errors = new ArrayList<>();
      try (ConcurrentQueryLoader loader = new ConcurrentQueryLoader(monitor, errors)) {
        for (int i = 0; i < 2000; i++) {
          loader.add(new MonitorQuery(Integer.toString(i), "\"test " + i + "\""));
        }
        assertTrue(errors.isEmpty());
      }

      assertEquals(2000, monitor.getQueryCount());

    }

  }

  public void testErrorHandling() throws Exception {

    try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), new MatchAllPresearcher())) {
      List<QueryError> errors = new ArrayList<>();
      try (ConcurrentQueryLoader loader = new ConcurrentQueryLoader(monitor, errors)) {
        for (int i = 0; i < 2000; i++) {
          String query = "test" + i;
          if (i % 200 == 0)
            query += " [";
          loader.add(new MonitorQuery(Integer.toString(i), query));
        }
      }

      assertEquals(10, errors.size());
      assertEquals(1990, monitor.getQueryCount());

    }

  }
}
