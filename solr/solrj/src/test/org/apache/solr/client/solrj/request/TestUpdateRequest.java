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

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestUpdateRequest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void expectException() {
    exception.expect(NullPointerException.class);
    exception.expectMessage("Cannot add a null SolrInputDocument");
  }

  @Test
  public void testCannotAddNullSolrInputDocument() {
    UpdateRequest req = new UpdateRequest();
    req.add((SolrInputDocument) null);
  }

  @Test
  public void testCannotAddNullDocumentWithOverwrite() {
    UpdateRequest req = new UpdateRequest();
    req.add(null, true);
  }

  @Test
  public void testCannotAddNullDocumentWithCommitWithin() {
    UpdateRequest req = new UpdateRequest();
    req.add(null, 1);
  }

  @Test
  public void testCannotAddNullDocumentWithParameters() {
    UpdateRequest req = new UpdateRequest();
    req.add(null, 1, true);
  }

  @Test
  public void testCannotAddNullDocumentAsPartOfList() {
    UpdateRequest req = new UpdateRequest();
    req.add(Arrays.asList(new SolrInputDocument(), new SolrInputDocument(), null));
  }

}
