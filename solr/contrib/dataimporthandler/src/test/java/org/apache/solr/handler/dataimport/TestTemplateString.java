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
package org.apache.solr.handler.dataimport;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * <p>
 * Test for TemplateString
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestTemplateString extends AbstractDataImportHandlerTestCase {
  @Test
  public void testSimple() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("last_index_time", Long.valueOf(1199429363730l));
    vri.addNamespace("indexer", ns);
    assertEquals("select id from subject where last_modified > 1199429363730",
                 new TemplateString().replaceTokens(
                   "select id from subject where last_modified > ${indexer.last_index_time}",
                   vri));
  }

  private static Properties EMPTY_PROPS = new Properties();

  private static Pattern SELECT_WHERE_PATTERN = Pattern.compile(
          "^\\s*(select\\b.*?\\b)(where).*", Pattern.CASE_INSENSITIVE);
}
