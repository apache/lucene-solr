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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.Test;

import static org.apache.solr.update.processor.IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.fastEstimate;

public class IgnoreLargeDocumentProcessorFactoryTest extends LuceneTestCase {

  @Test
  public void testProcessor() throws IOException {
    NamedList args = new NamedList();
    args.add(IgnoreLargeDocumentProcessorFactory.LIMIT_SIZE_PARAM, 1);

    IgnoreLargeDocumentProcessorFactory factory = new IgnoreLargeDocumentProcessorFactory();
    factory.init(args);
    try {
      UpdateRequestProcessor processor = factory.getInstance(null, null, null);
      processor.processAdd(getUpdate(1024));
      fail("Expected processor to ignore the update");
    } catch (SolrException e) {
      //expected
    }

    args = new NamedList();
    args.add(IgnoreLargeDocumentProcessorFactory.LIMIT_SIZE_PARAM, 2);
    factory = new IgnoreLargeDocumentProcessorFactory();
    factory.init(args);
    UpdateRequestProcessor processor = factory.getInstance(null, null, null);
    processor.processAdd(getUpdate(1024));

  }

  public AddUpdateCommand getUpdate(int size) {
    SolrInputDocument document = new SolrInputDocument();
    document.addField(new String(new byte[size], Charset.defaultCharset()), 1L);
    assertTrue(fastEstimate(document) > size);

    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = document;
    return cmd;
  }

  @Test
  public void testEstimateObjectSize() {
    assertEquals(fastEstimate("abc"), 6);
    assertEquals(fastEstimate("abcdefgh"), 16);
    List<String> keys = Arrays.asList("int", "long", "double", "float", "str");
    assertEquals(fastEstimate(keys), 42);
    List<Object> values = Arrays.asList(12, 5L, 12.0, 5.0, "duck");
    assertEquals(fastEstimate(values), 8);

    Map<String, Object> map = new HashMap<>();
    map.put("int", 12);
    map.put("long", 5L);
    map.put("double", 12.0);
    map.put("float", 5.0f);
    map.put("str", "duck");
    assertEquals(fastEstimate(map), 50);

    SolrInputDocument document = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      document.addField(entry.getKey(), entry.getValue());
    }
    assertEquals(fastEstimate(document), fastEstimate(map));

    SolrInputDocument childDocument = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      childDocument.addField(entry.getKey(), entry.getValue());
    }
    document.addChildDocument(childDocument);
    assertEquals(fastEstimate(document), fastEstimate(map) * 2);
  }
}
