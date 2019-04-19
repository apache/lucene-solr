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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.luwak.matchers.ExplainingMatch;
import org.apache.lucene.luwak.matchers.ExplainingMatcher;
import org.apache.lucene.search.Explanation;

import static org.hamcrest.CoreMatchers.containsString;

public class TestInputDocument extends MonitorTestBase {

  public void testCannotAddReservedFieldName() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> InputDocument.builder("id").addField(InputDocument.ID_FIELD, "test", new StandardAnalyzer()).build());
    assertThat(e.getMessage(), containsString("reserved"));
  }

  public void testCannotAddReservedFieldObject() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> InputDocument.builder("id").addField(new StringField(InputDocument.ID_FIELD, "", Field.Store.YES)).build());
    assertThat(e.getMessage(), containsString("reserved"));
  }

  public void testOmitNorms() throws Exception {
    FieldType type = new FieldType();
    type.setOmitNorms(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    Field f = new Field("text", "this is some text that will have a length norm greater than 0", type);

    InputDocument doc = InputDocument.builder("id")
        .setDefaultAnalyzer(new StandardAnalyzer())
        .addField(f).build();

    try (Monitor monitor = new Monitor()) {
      monitor.register(new MonitorQuery("q", parse("length")));

      Matches<ExplainingMatch> matches = monitor.match(doc, ExplainingMatcher.FACTORY);
      DocumentMatches<ExplainingMatch> m = matches.getMatches("id");
      for (ExplainingMatch e : m) {
        Explanation expl = e.getExplanation();
        assertThat(expl.toString(), containsString("1.0 = dl, length of field"));
      }
    }
  }

}
