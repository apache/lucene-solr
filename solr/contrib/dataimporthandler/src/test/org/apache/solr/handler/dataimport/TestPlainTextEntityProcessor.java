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

import java.io.StringReader;
import java.util.Properties;

/**
 * Test for PlainTextEntityProcessor
 *
 *
 * @see org.apache.solr.handler.dataimport.PlainTextEntityProcessor
 * @since solr 1.4
 */
public class TestPlainTextEntityProcessor extends AbstractDataImportHandlerTestCase {
  @Test
  public void testSimple() {
    DataImporter di = new DataImporter();
    di.loadAndInit(DATA_CONFIG);
    TestDocBuilder.SolrWriterImpl sw = new TestDocBuilder.SolrWriterImpl();
    DataImporter.RequestParams rp = new DataImporter.RequestParams(createMap("command", "full-import"));
    di.runCmd(rp, sw);
    assertEquals(DS.s, sw.docs.get(0).getFieldValue("x"));
  }

  public static class DS extends DataSource {
    static String s = "hello world";

    @Override
    public void init(Context context, Properties initProps) {

    }

    @Override
    public Object getData(String query) {

      return new StringReader(s);
    }

    @Override
    public void close() {

    }
  }

  static String DATA_CONFIG = "<dataConfig>\n" +
          "\t<dataSource type=\"TestPlainTextEntityProcessor$DS\" />\n" +
          "\t<document>\n" +
          "\t\t<entity processor=\"PlainTextEntityProcessor\" name=\"x\" query=\"x\">\n" +
          "\t\t\t<field column=\"plainText\" name=\"x\" />\n" +
          "\t\t</entity>\n" +
          "\t</document>\n" +
          "</dataConfig>";
}
