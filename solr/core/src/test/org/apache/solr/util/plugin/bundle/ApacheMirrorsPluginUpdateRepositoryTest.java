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

package org.apache.solr.util.plugin.bundle;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test apache mirror download
 */
public class ApacheMirrorsPluginUpdateRepositoryTest {
  private ApacheMirrorsUpdateRepository mirrorExist;
  private ApacheMirrorsUpdateRepository repoRedir;
  private ApacheMirrorsUpdateRepository nonexist;

  // TODO: Mock

  @Before
  public void setUp() throws Exception {
    mirrorExist = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/6.5.0");
    repoRedir = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/5.5.0");
    nonexist = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/nonExist");
  }

  @Test
  public void mirrorExist() throws Exception {
    assertEquals("http://apache.uib.no/lucene/solr/6.5.0/", mirrorExist.getUrl().toString());
  }

  @Test
  public void successfulRedirect() throws Exception {
    assertEquals("https://archive.apache.org/dist/lucene/solr/5.5.0/", repoRedir.getUrl().toString());
  }

  @Test
  public void nonExistingRedir() throws Exception {
    assertEquals(null, nonexist.getUrl());
  }

  @Test
  public void getPlugins() throws Exception {
    assertEquals(0, mirrorExist.getPlugins().size());
  }

  @Test
  public void testMd5() throws Exception {
    // TODO: Mock
//    Path file = mirrorExist.getFileDownloader().downloadFile(new URL(mirrorExist.getUrl() + "file"));
//    assertTrue(Files.exists(file));
  }
}