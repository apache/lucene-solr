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

import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.update.UpdateRepository;

public class UpdateRepositoryFactory {
  
  public static UpdateRepository create(String id, String urlAsString) throws PluginException {
    try {
      URL url = new URL(urlAsString);
      if (url.getHost().endsWith("github.com")) {
        if (url.getPath().split("/").length < 2) {
          throw new PluginException("Invalid URL for github repo: " + url);
        }
        return new GitHubUpdateRepository(id,
            url.getPath().split("/")[0], 
            url.getPath().split("/")[1]);
      } else if (url.getHost().endsWith("apache.org") && url.getPath().startsWith("dist")) {
        if (url.getPath().split("/").length < 2) {
          throw new PluginException("Invalid URL for Apache mirror repo: " + url);
        }
        return new ApacheMirrorsUpdateRepository(id,
            url.getPath().split("/")[1]);
      } else {
        return new PluginUpdateRepository(id, url);
      }
    } catch (MalformedURLException e) {
      throw new PluginException("Failed to create repository from URL " + urlAsString, e);
    }
  }
}
