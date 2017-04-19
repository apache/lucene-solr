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

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.solr.common.SolrException;

/**
 * Loads plugins from a GitHub repo given its URL or simpply "user/repo"
 */
public class GitHubUpdateRepository extends PluginUpdateRepository {
  private static final String GITHUB_RAW_ROOT = "https://raw.githubusercontent.com/{user}/{repo}/master/";
  private final String githubUser;
  private final String repo;

  public GitHubUpdateRepository(String id, String gitHubUser, String repo) {
    super(id, null);
    this.githubUser = gitHubUser;
    this.repo = repo;
  }

  /**
   * Returns the GitHub RAW url
   * @return Url of the repository location
   */
  @Override
  protected URL resolveUrl() {
    try {
      return new URL(GITHUB_RAW_ROOT.replaceFirst("\\{user\\}", githubUser).replaceFirst("\\{repo\\}", repo));
    } catch (MalformedURLException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
}
