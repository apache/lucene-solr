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
package org.apache.solr.core;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.core.backup.repository.HdfsBackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.schema.FieldType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class TestBackupRepositoryFactory extends SolrTestCaseJ4 {
  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  // tmp dir, cleaned up automatically.
  private static File solrHome = null;
  private static SolrResourceLoader loader = null;

  @BeforeClass
  public static void setupLoader() throws Exception {
    solrHome = createTempDir().toFile();
    loader = new SolrResourceLoader(solrHome.toPath());
  }

  @AfterClass
  public static void cleanupLoader() throws Exception {
    solrHome = null;
    loader = null;
  }

  @Test
  public void testMultipleDefaultRepositories() {
    PluginInfo[] plugins = new PluginInfo[2];

    {
      Map<String, Object> attrs = new HashMap<>();
      attrs.put(CoreAdminParams.NAME, "repo1");
      attrs.put(FieldType.CLASS_NAME, "a.b.C");
      attrs.put("default" , "true");
      plugins[0] = new PluginInfo("repository", attrs);
    }

    {
      Map<String, Object> attrs = new HashMap<>();
      attrs.put(CoreAdminParams.NAME, "repo2");
      attrs.put(FieldType.CLASS_NAME, "p.q.R");
      attrs.put("default" , "true");
      plugins[1] = new PluginInfo("repository", attrs);
    }

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("More than one backup repository is configured as default");
    new BackupRepositoryFactory(plugins);
  }

  @Test
  public void testMultipleRepositoriesWithSameName() {
    PluginInfo[] plugins = new PluginInfo[2];

    {
      Map<String, Object> attrs = new HashMap<>();
      attrs.put(CoreAdminParams.NAME, "repo1");
      attrs.put(FieldType.CLASS_NAME, "a.b.C");
      attrs.put("default" , "true");
      plugins[0] = new PluginInfo("repository", attrs);
    }

    {
      Map<String, Object> attrs = new HashMap<>();
      attrs.put(CoreAdminParams.NAME, "repo1");
      attrs.put(FieldType.CLASS_NAME, "p.q.R");
      plugins[1] = new PluginInfo("repository", attrs);
    }

    expectedException.expect(SolrException.class);
    expectedException.expectMessage("Duplicate backup repository with name repo1");
    new BackupRepositoryFactory(plugins);
  }

  @Test
  public void testNonExistantBackupRepository() {
    PluginInfo[] plugins = new PluginInfo[0];
    BackupRepositoryFactory f = new BackupRepositoryFactory(plugins);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Could not find a backup repository with name repo1");
    f.newInstance(loader, "repo1");
  }

  @Test
  public void testRepositoryConfig() {
    PluginInfo[] plugins = new PluginInfo[2];

    {
      Map<String, Object> attrs = new HashMap<>();
      attrs.put(CoreAdminParams.NAME, "repo1");
      attrs.put(FieldType.CLASS_NAME, LocalFileSystemRepository.class.getName());
      attrs.put("default" , "true");
      attrs.put("location", "/tmp");
      plugins[0] = new PluginInfo("repository", attrs);
    }
    {
      Map<String, Object> attrs = new HashMap<>();
      attrs.put(CoreAdminParams.NAME, "boom");
      attrs.put(FieldType.CLASS_NAME, HdfsBackupRepository.class.getName());
      attrs.put("location", "/tmp");
      plugins[1] = new PluginInfo("repository", attrs);
    }
    Collections.shuffle(Arrays.asList(plugins), random());

    BackupRepositoryFactory f = new BackupRepositoryFactory(plugins);

    {
      BackupRepository repo = f.newInstance(loader);

      assertTrue(repo instanceof LocalFileSystemRepository);
      assertEquals("/tmp", repo.getConfigProperty("location"));
    }

    {
      BackupRepository repo = f.newInstance(loader, "repo1");

      assertTrue(repo instanceof LocalFileSystemRepository);
      assertEquals("/tmp", repo.getConfigProperty("location"));
    }
    
    {
      try {
        BackupRepository repo = f.newInstance(loader, "boom");
        fail();
      } catch (Exception e) {
      }
    }
  }
}
