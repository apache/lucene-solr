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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Explanation;

public class TestExplainingMatcher extends MonitorTestBase {

  public void testExplainingMatcher() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(
          new MonitorQuery("1", parse("test")), new MonitorQuery("2", parse("wibble")));

      Document doc = new Document();
      doc.add(newTextField("field", "test", Field.Store.NO));

      MatchingQueries<ExplainingMatch> matches = monitor.match(doc, ExplainingMatch.MATCHER);
      assertNotNull(matches.matches("1"));
      assertNotNull(matches.matches("1").getExplanation());
    }
  }

  public void testHashcodeAndEquals() {

    ExplainingMatch m1 = new ExplainingMatch("1", Explanation.match(0.1f, "an explanation"));
    ExplainingMatch m3 = new ExplainingMatch("1", Explanation.match(0.1f, "another explanation"));
    ExplainingMatch m4 = new ExplainingMatch("1", Explanation.match(0.1f, "an explanation"));

    assertEquals(m1, m4);
    assertEquals(m1.hashCode(), m4.hashCode());
    assertNotEquals(m1, m3);
    assertNotEquals(m3, m4);
  }
}
