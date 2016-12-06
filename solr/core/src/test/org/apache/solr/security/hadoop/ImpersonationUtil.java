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
package org.apache.solr.security.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.security.HadoopAuthPlugin;
import org.apache.solr.security.KerberosPlugin;

/**
 * This class implements utility functions required to test the secure impersonation feature for {@linkplain HadoopAuthPlugin}
 */
public class ImpersonationUtil {

  static String getUsersFirstGroup() throws Exception {
    String group = "*"; // accept any group if a group can't be found
    if (!Constants.WINDOWS) { // does not work on Windows!
      org.apache.hadoop.security.Groups hGroups =
          new org.apache.hadoop.security.Groups(new Configuration());
      try {
        List<String> g = hGroups.getGroups(System.getProperty("user.name"));
        if (g != null && g.size() > 0) {
          group = g.get(0);
        }
      } catch (NullPointerException npe) {
        // if user/group doesn't exist on test box
      }
    }
    return group;
  }

  static SolrRequest getProxyRequest(String user, String doAs) {
    return new CollectionAdminRequest.List() {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
        params.set(PseudoAuthenticator.USER_NAME, user);
        params.set(KerberosPlugin.IMPERSONATOR_DO_AS_HTTP_PARAM, doAs);
        return params;
      }
    };
  }

  static String getExpectedGroupExMsg(String user, String doAs) {
    return "User: " + user + " is not allowed to impersonate " + doAs;
  }

  static String getExpectedHostExMsg(String user) {
    return "Unauthorized connection for super-user: " + user;
  }

}
