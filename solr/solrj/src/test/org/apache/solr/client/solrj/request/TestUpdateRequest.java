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
package org.apache.solr.client.solrj.request;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.solr.common.LinkedHashMapWriter;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.solr.SolrTestCaseJ4.adoc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestUpdateRequest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void expectException() {
  }

  @Test
  public void testCannotAddNullSolrInputDocument() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Cannot add a null SolrInputDocument");

    UpdateRequest req = new UpdateRequest();
    req.add((SolrInputDocument) null);
  }

  @Test
  public void testCannotAddNullDocumentWithOverwrite() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Cannot add a null SolrInputDocument");

    UpdateRequest req = new UpdateRequest();
    req.add(null, true);
  }

  @Test
  public void testCannotAddNullDocumentWithCommitWithin() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Cannot add a null SolrInputDocument");

    UpdateRequest req = new UpdateRequest();
    req.add(null, 1);
  }

  @Test
  public void testCannotAddNullDocumentWithParameters() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Cannot add a null SolrInputDocument");

    UpdateRequest req = new UpdateRequest();
    req.add(null, 1, true);
  }

  @Test
  public void testCannotAddNullDocumentAsPartOfList() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Cannot add a null SolrInputDocument");

    UpdateRequest req = new UpdateRequest();
    req.add(Arrays.asList(new SolrInputDocument(), new SolrInputDocument(), null));
  }

  @Test
  public void testEqualsMethod() {
    final SolrInputDocument doc1 = new SolrInputDocument("id", "1", "value_s", "foo");
    final SolrInputDocument doc2 = new SolrInputDocument("id", "2", "value_s", "bar");
    final SolrInputDocument doc3 = new SolrInputDocument("id", "3", "value_s", "baz");

    // Different Added/Updated Documents
    {
      final UpdateRequest req1 = new UpdateRequest();
      req1.add(doc1);
      final UpdateRequest req2 = new UpdateRequest();
      req2.add(doc2);

      assertNotEquals(req1, req2);
    }

    // Different Added/Updated DocIterator
    {
      final List<SolrInputDocument> list1 = Lists.newArrayList(doc1, doc2);
      final List<SolrInputDocument> list2 = Lists.newArrayList(doc2, doc3);

      final UpdateRequest req1 = new UpdateRequest();
      req1.setDocIterator(list1.iterator());
      final UpdateRequest req2 = new UpdateRequest();
      req2.setDocIterator(list2.iterator());

      assertNotEquals(req1, req2);
    }

    // NOCOMMIT - create jira for the NPE that occurs when the iterator has no documents (see example above

    // Different IDs to delete
    {
      final UpdateRequest req1 = new UpdateRequest();
      req1.deleteById("id1");
      final UpdateRequest req2 = new UpdateRequest();
      req2.deleteById("id2");

      assertNotEquals(req1, req2);
    }

    // Different DBQ queries
    {
      final UpdateRequest req1 = new UpdateRequest();
      req1.deleteByQuery("inStock:false");
      final UpdateRequest req2 = new UpdateRequest();
      req2.deleteByQuery("price_i:[5 TO 10]");

      assertNotEquals(req1, req2);
    }

    // Same DBQ queries in different order
    {
      final UpdateRequest req1 = new UpdateRequest();
      req1.deleteByQuery("inStock:false");
      req1.deleteByQuery("price_i:[5 TO 10]");
      final UpdateRequest req2 = new UpdateRequest();
      req2.deleteByQuery("price_i:[5 TO 10]");
      req2.deleteByQuery("inStock:false");

      assertNotEquals(req1, req2);
    }

    // Different inherited values
    {
      final UpdateRequest req1 = new UpdateRequest();
      req1.setBasePath("/update2");
      final UpdateRequest req2 = new UpdateRequest();
      req2.setBasePath("/update");

      assertNotEquals(req1, req2);
    }

    // Identical objects
    {
      final UpdateRequest req1 = createComplicatedUpdate();
      final UpdateRequest req2 = createComplicatedUpdate();

      assertEquals(req1, req2);
    }
  }

  private UpdateRequest createComplicatedUpdate() {
    final UpdateRequest req = new UpdateRequest();
    req.add("id", "1", "value_s", "2");
    req.add("id", "2", "value_s", "3");
    req.deleteByQuery("inStock:false");
    req.deleteById("5", "6");
    req.setBasePath("/update2");
    req.setParam("someRandomParam", "someRandomValue");

    return req;
  }
}
