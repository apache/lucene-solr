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
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallbackAuditLoggerPlugin extends AuditLoggerPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private int callbackPort;
  private Socket socket;
  private PrintWriter out;

  /**
   * Opens a socket to send a callback, e.g. to a running test client
   * @param event the audit event
   */
  @Override
  public void audit(AuditEvent event) {
    log.info("Received audit event, type={}", event.getEventType());
    out.append('l');
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    super.init(pluginConfig);
    callbackPort = Integer.parseInt((String) pluginConfig.get("callbackPort"));
    try {
      socket = new Socket("127.0.0.1", callbackPort);
//      socket.bind(new InetSocketAddress(InetAddress.getLocalHost(), callbackPort));
      out = new PrintWriter(socket.getOutputStream(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException { 
    socket.close();
  }
}
