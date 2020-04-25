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
package org.apache.solr.security;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Special test-only audit logger which will send the path (e.g. /select) as a callback to the running test
 */
public class CallbackAuditLoggerPlugin extends AuditLoggerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final Map<String,Semaphore> BLOCKING_SEMAPHORES = new HashMap<>();
  
  private int callbackPort;
  private Socket socket;
  private PrintWriter out;
  private Semaphore semaphore = null;
    
  /**
   * Opens a socket to send a callback, e.g. to a running test client
   * @param event the audit event
   */
  @Override
  public void audit(AuditEvent event) {
    if (null != semaphore) {
      log.info("Waiting to acquire ticket from semaphore");
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        log.warn("audit() interrupted while waiting for ticket, probably due to shutdown, aborting");
        return;
      }
    }
    out.write(formatter.formatEvent(event) + "\n");
    if (! out.checkError()) {
      log.error("Output stream has an ERROR!");
    }
    log.info("Sent audit callback {} to localhost:{}", formatter.formatEvent(event), callbackPort);
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    super.init(pluginConfig);
    callbackPort = Integer.parseInt((String) pluginConfig.get("callbackPort"));
    final String semaphoreName = (String) pluginConfig.get("semaphore");
    if (null != semaphoreName) {
      semaphore = BLOCKING_SEMAPHORES.get(semaphoreName);
      if (null == semaphore) {
        throw new RuntimeException("Test did not setup semaphore of specified name: " + semaphoreName);
      }
    }
    try {
      socket = new Socket("localhost", callbackPort);
      out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException { 
    super.close();
    if (socket != null) socket.close();
  }
}
