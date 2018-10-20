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
package org.apache.lucene.replicator.http;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

import org.apache.http.HttpResponse;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.lucene.replicator.Replicator;
import org.apache.lucene.replicator.Revision;
import org.apache.lucene.replicator.SessionToken;
import org.apache.lucene.replicator.http.ReplicationService.ReplicationAction;

/**
 * An HTTP implementation of {@link Replicator}. Assumes the API supported by
 * {@link ReplicationService}.
 * 
 * @lucene.experimental
 */
public class HttpReplicator extends HttpClientBase implements Replicator {
  
  /** Construct with specified connection manager. */
  public HttpReplicator(String host, int port, String path, HttpClientConnectionManager conMgr) {
    super(host, port, path, conMgr, null);
  }
  
  @Override
  public SessionToken checkForUpdate(String currVersion) throws IOException {
    String[] params = null;
    if (currVersion != null) {
      params = new String[] { ReplicationService.REPLICATE_VERSION_PARAM, currVersion };
    }
    final HttpResponse response = executeGET(ReplicationAction.UPDATE.name(), params);
    return doAction(response, new Callable<SessionToken>() {
      @Override
      public SessionToken call() throws Exception {
        final DataInputStream dis = new DataInputStream(responseInputStream(response));
        try {
          if (dis.readByte() == 0) {
            return null;
          } else {
            return new SessionToken(dis);
          }
        } finally {
          dis.close();
        }
      }
    });
  }
  
  @Override
  public InputStream obtainFile(String sessionID, String source, String fileName) throws IOException {
    String[] params = new String[] {
        ReplicationService.REPLICATE_SESSION_ID_PARAM, sessionID,
        ReplicationService.REPLICATE_SOURCE_PARAM, source,
        ReplicationService.REPLICATE_FILENAME_PARAM, fileName,
    };
    final HttpResponse response = executeGET(ReplicationAction.OBTAIN.name(), params);
    return doAction(response, false, new Callable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        return responseInputStream(response, true);
      }
    });
  }
  
  @Override
  public void publish(Revision revision) throws IOException {
    throw new UnsupportedOperationException(
        "this replicator implementation does not support remote publishing of revisions");
  }
  
  @Override
  public void release(String sessionID) throws IOException {
    String[] params = new String[] {
        ReplicationService.REPLICATE_SESSION_ID_PARAM, sessionID
    };
    final HttpResponse response = executeGET(ReplicationAction.RELEASE.name(), params);
    doAction(response, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return null; // do not remove this call: as it is still validating for us!
      }
    });
  }
  
}
