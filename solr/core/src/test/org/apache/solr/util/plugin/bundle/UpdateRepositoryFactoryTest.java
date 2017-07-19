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

import org.junit.Test;
import ro.fortsoft.pf4j.PluginRepository;
import ro.fortsoft.pf4j.update.UpdateRepository;

import static org.junit.Assert.*;

/**
 * Test the repo factory
 */
public class UpdateRepositoryFactoryTest {
  
  @Test
  public void testFactoryForGithub() throws Exception {
    UpdateRepository repo = UpdateRepositoryFactory.create("git", "https://github.com/cominvent/solr-plugins");
    assertEquals(GitHubUpdateRepository.class, repo.getClass());
    assertEquals("https://raw.githubusercontent.com/cominvent/solr-plugins/master/", repo.getUrl().toString());
  }

  @Test
  public void testFactoryForApache() throws Exception {
    UpdateRepository repo = UpdateRepositoryFactory.create("apache", "https://www.apache.org/dist/lucene/solr/");
    assertEquals(ApacheMirrorsUpdateRepository.class, repo.getClass());
    assertEquals("https://www.apache.org/dyn/closer.lua?action=download&filename=lucene/solr/", 
        ((ApacheMirrorsUpdateRepository)repo).getMirrorUrl().toString());
  }

  @Test
  public void testFactoryForOthers() throws Exception {
    UpdateRepository repo = UpdateRepositoryFactory.create("apache", "https://www.example.com/my/repo/");
    assertEquals(PluginUpdateRepository.class, repo.getClass());
  }
}