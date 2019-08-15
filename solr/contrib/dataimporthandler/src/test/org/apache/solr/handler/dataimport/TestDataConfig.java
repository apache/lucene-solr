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
package org.apache.solr.handler.dataimport;

import org.apache.solr.handler.dataimport.config.DIHConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Test for DataConfig
 * </p>
 *
 *
 * @since solr 1.3
 */
public class TestDataConfig extends AbstractDataImportHandlerTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-nodatasource-solrconfig.xml", "dataimport-schema.xml");
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testDataConfigWithDataSource() throws Exception {
    List rows = new ArrayList();
    rows.add(createMap("id", "1", "desc", "one"));
    MockDataSource.setIterator("select * from x", rows.iterator());

    runFullImport(loadDataConfig("data-config-with-datasource.xml"));

    assertQ(req("id:1"), "//*[@numFound='1']");
  }

  @Test
  public void testBasic() throws Exception {
    javax.xml.parsers.DocumentBuilder builder = DocumentBuilderFactory
            .newInstance().newDocumentBuilder();
    Document doc = builder.parse(new InputSource(new StringReader(xml)));
    DataImporter di = new DataImporter();
    DIHConfiguration dc = di.readFromXml(doc);
    assertEquals("atrimlisting", dc.getEntities().get(0).getName());
  }

  private static final String xml = "<dataConfig>\n"
          + "\t<document name=\"autos\" >\n"
          + "\t\t<entity name=\"atrimlisting\" pk=\"acode\"\n"
          + "\t\t\tquery=\"select acode,make,model,year,msrp,category,image,izmo_image_url,price_range_low,price_range_high,invoice_range_low,invoice_range_high from atrimlisting\"\n"
          + "\t\t\tdeltaQuery=\"select acode from atrimlisting where last_modified > '${indexer.last_index_time}'\">\n"
          +

          "\t\t</entity>\n" +

          "\t</document>\n" + "</dataConfig>";
}
