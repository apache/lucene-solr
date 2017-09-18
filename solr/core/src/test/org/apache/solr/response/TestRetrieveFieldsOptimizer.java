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

package org.apache.solr.response;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class TestRetrieveFieldsOptimizer extends SolrTestCaseJ4{

  @Test
  public void testOptimizer() {
    RetrieveFieldsOptimizer optimizer = new RetrieveFieldsOptimizer(
        new HashSet<>(Arrays.asList("id", "title")),
        new HashSet<>()
    );
    optimizer.optimize(new HashSet<>(Arrays.asList("id", "title")));
    assertTrue(optimizer.returnDVFields());
    assertFalse(optimizer.returnStoredFields());

    optimizer = new RetrieveFieldsOptimizer(
        new HashSet<>(Arrays.asList("id", "title")),
        new HashSet<>()
    );
    optimizer.optimize(new HashSet<>(Collections.singletonList("title")));
    assertFalse(optimizer.returnDVFields());
    assertTrue(optimizer.returnStoredFields());

    optimizer = new RetrieveFieldsOptimizer(
        null,
        new HashSet<>(Collections.singletonList("id"))
    );
    optimizer.optimize(new HashSet<>(Collections.singletonList("id")));
    assertNull(optimizer.getStoredFields());
    assertTrue(optimizer.getDvFields().contains("id"));

  }
}
