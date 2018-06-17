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
package org.apache.solr.handler;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

public class CheckBackupStatus extends SolrTestCaseJ4 {
  String response = null;
  public boolean success = false;
  String backupTimestamp = null;
  final String lastBackupTimestamp;
  final Pattern p = Pattern.compile("<str name=\"snapshotCompletedAt\">(.*?)</str>");
  final Pattern pException = Pattern.compile("<str name=\"snapShootException\">(.*?)</str>");
  final Http2SolrClient client;
  final String coreName;

  public CheckBackupStatus(final Http2SolrClient client, String coreName, String lastBackupTimestamp) {
    this.client = client;
    this.lastBackupTimestamp = lastBackupTimestamp;
    this.coreName = coreName;
  }

  public CheckBackupStatus(final Http2SolrClient client, String coreName) {
    this(client, coreName, null);
  }

  public void fetchStatus() throws Exception {
    String masterUrl = client.getBaseURL() + "/"  + coreName + ReplicationHandler.PATH + "?wt=xml&command=" + ReplicationHandler.CMD_DETAILS;
    response = client.httpGet(masterUrl).asString;
    if(pException.matcher(response).find()) {
      fail("Failed to create backup");
    }
    if(response.contains("<str name=\"status\">success</str>")) {
      Matcher m = p.matcher(response);
      if(!m.find()) {
        fail("could not find the completed timestamp in response.");
      }
      if (lastBackupTimestamp != null) {
        backupTimestamp = m.group(1);
        if (backupTimestamp.equals(lastBackupTimestamp)) {
          success = true;
        }
      } else {
        success = true;
      }
    }
  }
}
