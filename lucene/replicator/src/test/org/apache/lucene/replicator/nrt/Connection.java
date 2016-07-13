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

package org.apache.lucene.replicator.nrt;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;

/** Simple point-to-point TCP connection */
class Connection implements Closeable {
  public final DataInput in;
  public final DataOutput out;
  public final InputStream sockIn;
  public final BufferedOutputStream bos;
  public final Socket s;
  public final int destTCPPort;
  public long lastKeepAliveNS = System.nanoTime();

  public Connection(int tcpPort) throws IOException {
    this.destTCPPort = tcpPort;
    this.s = new Socket(InetAddress.getLoopbackAddress(), tcpPort);
    this.sockIn = s.getInputStream();
    this.in = new InputStreamDataInput(sockIn);
    this.bos = new BufferedOutputStream(s.getOutputStream());
    this.out = new OutputStreamDataOutput(bos);
    if (Node.VERBOSE_CONNECTIONS) {
      System.out.println("make new client Connection socket=" + this.s + " destPort=" + tcpPort);
    }
  }

  public void flush() throws IOException {
    bos.flush();
  }

  @Override
  public void close() throws IOException {
    s.close();
  }
}
