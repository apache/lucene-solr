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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Special test-only audit logger which will send the path (e.g. /select) as a callback to the running test
 */
public class CallbackAuditLoggerPlugin extends AuditLoggerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private int callbackPort;
  private Socket socket;
  private PrintWriter out;
  private int delay;

  /**
   * Opens a socket to send a callback, e.g. to a running test client
   * @param event the audit event
   */
  @Override
  public void audit(AuditEvent event) {
    if (delay > 0) {
      log.info("Sleeping for {}ms before sending callback", delay);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
    }
    out.write(formatter.formatEvent(event) + "\n");
    out.flush();
    log.info("Sent audit callback {} to localhost:{}", formatter.formatEvent(event), callbackPort);
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    super.init(pluginConfig);
    callbackPort = Integer.parseInt((String) pluginConfig.get("callbackPort"));
    delay = Integer.parseInt((String) pluginConfig.get("delay"));
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
