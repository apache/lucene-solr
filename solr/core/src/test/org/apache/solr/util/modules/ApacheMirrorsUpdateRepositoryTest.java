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

package org.apache.solr.util.modules;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test apache mirror download
 */
public class ApacheMirrorsUpdateRepositoryTest {
  private ApacheMirrorsUpdateRepository mirrorExist;
  private ApacheMirrorsUpdateRepository repoRedir;
  private ApacheMirrorsUpdateRepository nonexist;

  // TODO: Mock

  @Before
  public void setUp() throws Exception {
    mirrorExist = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/6.5.0", "modules.json");
    repoRedir = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/5.5.0", "modules.json");
    nonexist = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/nonExist", "modules.json");
  }

  @Test
  public void mirrorExist() throws Exception {
    assertEquals("http://apache.uib.no/lucene/solr/6.5.0/", mirrorExist.getLocation());
  }

  @Test
  public void successfulRedirect() throws Exception {
    assertEquals("https://archive.apache.org/dist/lucene/solr/5.5.0/", repoRedir.getLocation());
  }

  @Test
  public void nonExistingRedir() throws Exception {
    assertEquals(null, nonexist.getLocation());
  }

  @Test
  public void getPlugins() throws Exception {
    assertEquals(0, mirrorExist.getPlugins().size());
  }

}