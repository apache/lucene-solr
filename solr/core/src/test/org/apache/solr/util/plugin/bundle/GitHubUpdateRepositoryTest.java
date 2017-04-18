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

import static org.junit.Assert.*;

/**
 * Test the GitHub repo
 */
public class GitHubUpdateRepositoryTest {
  private GitHubUpdateRepository repo;

  @Before
  public void init() {
    repo = new GitHubUpdateRepository("github","myUser", "myRepo");
  }

  @Test
  public void resolveUrl() throws Exception {
    assertEquals("https://raw.githubusercontent.com/myUser/myRepo/master/", repo.resolveUrl());
  }

}