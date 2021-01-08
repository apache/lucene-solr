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

import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class TestPresearcherMatchCollector extends MonitorTestBase {

  public void testMatchCollectorShowMatches() throws IOException {

    try (Monitor monitor = new Monitor(ANALYZER, new TermFilteredPresearcher())) {
      monitor.register(new MonitorQuery("1", parse("test")));
      monitor.register(new MonitorQuery("2", parse("foo bar -baz f2:quuz")));
      monitor.register(new MonitorQuery("3", parse("foo -test")));
      monitor.register(new MonitorQuery("4", parse("baz")));
      assertEquals(4, monitor.getQueryCount());

      Document doc = new Document();
      doc.add(newTextField(FIELD, "this is a foo test", Field.Store.NO));
      doc.add(newTextField("f2", "quuz", Field.Store.NO));

      PresearcherMatches<QueryMatch> matches = monitor.debug(doc, QueryMatch.SIMPLE_MATCHER);

      assertNotNull(matches.match("1", 0));
      assertEquals(" field:test", matches.match("1", 0).presearcherMatches);
      assertNotNull(matches.match("1", 0).queryMatch);

      assertNotNull(matches.match("2", 0));
      String pm = matches.match("2", 0).presearcherMatches;
      assertThat(pm, containsString("field:foo"));
      assertThat(pm, containsString("f2:quuz"));

      assertNotNull(matches.match("3", 0));
      assertEquals(" field:foo", matches.match("3", 0).presearcherMatches);
      assertNull(matches.match("3", 0).queryMatch);

      assertNull(matches.match("4", 0));
    }
  }
}
