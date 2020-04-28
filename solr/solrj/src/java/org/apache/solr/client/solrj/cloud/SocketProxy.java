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
package org.apache.solr.client.solrj.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kindly borrowed the idea and base implementation from the ActiveMQ project;
 * useful for blocking traffic on a specified port.
 */
public class SocketProxy {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final int ACCEPT_TIMEOUT_MILLIS = 100;

  // should be as large as the HttpShardHandlerFactory socket timeout ... or larger?
  public static final int PUMP_SOCKET_TIMEOUT_MS = 100 * 1000;
  
  private URI proxyUrl;
  private URI target;
  
  private Acceptor acceptor;
  private ServerSocket serverSocket;
  
  private CountDownLatch closed = new CountDownLatch(1);
  
  public List<Bridge> connections = new LinkedList<Bridge>();
  
  private final int listenPort;
  
  private int receiveBufferSize = -1;
  
  private boolean pauseAtStart = false;
  
  private int acceptBacklog = 50;

  private boolean usesSSL;

  public SocketProxy() throws Exception {
    this(0, false);
  }
  
  public SocketProxy( boolean useSSL) throws Exception {
    this(0, useSSL);
  }
  
  public SocketProxy(int port, boolean useSSL) throws Exception {
    int listenPort = port;
    this.usesSSL = useSSL;
    serverSocket = createServerSocket(useSSL);
    serverSocket.setReuseAddress(true);
    if (receiveBufferSize > 0) {
      serverSocket.setReceiveBufferSize(receiveBufferSize);
    }
    serverSocket.bind(new InetSocketAddress(listenPort), acceptBacklog);
    this.listenPort = serverSocket.getLocalPort();
  }
  
  public void open(URI uri) throws Exception {
    target = uri;
    proxyUrl = urlFromSocket(target, serverSocket);
    doOpen();
  }
  
  public String toString() {
    return "SocketyProxy: port="+listenPort+"; target="+target;
  }
    
  public void setReceiveBufferSize(int receiveBufferSize) {
    this.receiveBufferSize = receiveBufferSize;
  }
  
  public void setTarget(URI tcpBrokerUri) {
    target = tcpBrokerUri;
  }
  
  private void doOpen() throws Exception {
    
    acceptor = new Acceptor(serverSocket, target);
    if (pauseAtStart) {
      acceptor.pause();
    }
    new Thread(null, acceptor, "SocketProxy-Acceptor-"
        + serverSocket.getLocalPort()).start();
    closed = new CountDownLatch(1);
  }
  
  public int getListenPort() {
    return listenPort;
  }
  
  private ServerSocket createServerSocket(boolean useSSL) throws Exception {
    if (useSSL) {
      return SSLServerSocketFactory.getDefault().createServerSocket();
    }
    return new ServerSocket();
  }
  
  private Socket createSocket(boolean useSSL) throws Exception {
    if (useSSL) {
      return SSLSocketFactory.getDefault().createSocket();
    }
    return new Socket();
  }
  
  public URI getUrl() {
    return proxyUrl;
  }
  
  /*
   * close all proxy connections and acceptor
   */
  public void close() {
    List<Bridge> connections;
    synchronized (this.connections) {
      connections = new ArrayList<Bridge>(this.connections);
    }
    log.warn("Closing {} connections to: {}, target: {}", connections.size(), getUrl(), target);
    for (Bridge con : connections) {
      closeConnection(con);
    }
    acceptor.close();
    closed.countDown();
  }
  
  /*
   * close all proxy receive connections, leaving acceptor open
   */
  public void halfClose() {
    List<Bridge> connections;
    synchronized (this.connections) {
      connections = new ArrayList<Bridge>(this.connections);
    }
    if (log.isInfoEnabled()) {
      log.info("halfClose, numConnections= {}", connections.size());
    }
    for (Bridge con : connections) {
      halfCloseConnection(con);
    }
  }
  
  public boolean waitUntilClosed(long timeoutSeconds)
      throws InterruptedException {
    return closed.await(timeoutSeconds, TimeUnit.SECONDS);
  }
  
  /*
   * called after a close to restart the acceptor on the same port
   */
  public void reopen() {
    if (log.isInfoEnabled()) {
      log.info("Re-opening connectivity to {}", getUrl());
    }
    try {
      if (proxyUrl == null) {
        throw new IllegalStateException("Can not call open before open(URI uri).");
      }
      serverSocket = createServerSocket(usesSSL);
      serverSocket.setReuseAddress(true);
      if (receiveBufferSize > 0) {
        serverSocket.setReceiveBufferSize(receiveBufferSize);
      }
      serverSocket.bind(new InetSocketAddress(proxyUrl.getPort()));
      doOpen();
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("exception on reopen url:{} ", getUrl(), e);
      }
    }
  }
  
  /*
   * pause accepting new connections and data transfer through existing proxy
   * connections. All sockets remain open
   */
  public void pause() {
    synchronized (connections) {
      if (log.isInfoEnabled()) {
        log.info("pause, numConnections={}", connections.size());
      }
      acceptor.pause();
      for (Bridge con : connections) {
        con.pause();
      }
    }
  }
  
  /*
   * continue after pause
   */
  public void goOn() {
    synchronized (connections) {
      if (log.isInfoEnabled()) {
        log.info("goOn, numConnections={}", connections.size());
      }
      for (Bridge con : connections) {
        con.goOn();
      }
    }
    acceptor.goOn();
  }
  
  private void closeConnection(Bridge c) {
    try {
      c.close();
    } catch (Exception e) {
      log.debug("exception on close of: {}", c, e);
    }
  }
  
  private void halfCloseConnection(Bridge c) {
    try {
      c.halfClose();
    } catch (Exception e) {
      log.debug("exception on half close of: {}", c, e);
    }
  }
  
  public boolean isPauseAtStart() {
    return pauseAtStart;
  }
  
  public void setPauseAtStart(boolean pauseAtStart) {
    this.pauseAtStart = pauseAtStart;
  }
  
  public int getAcceptBacklog() {
    return acceptBacklog;
  }
  
  public void setAcceptBacklog(int acceptBacklog) {
    this.acceptBacklog = acceptBacklog;
  }
  
  private URI urlFromSocket(URI uri, ServerSocket serverSocket)
      throws Exception {
    int listenPort = serverSocket.getLocalPort();
    
    return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
        listenPort, uri.getPath(), uri.getQuery(), uri.getFragment());
  }
  
  public class Bridge {
    
    private Socket receiveSocket;
    private Socket sendSocket;
    private Pump requestThread;
    private Pump responseThread;
    
    public Bridge(Socket socket, URI target) throws Exception {
      receiveSocket = socket;
      sendSocket = createSocket(usesSSL);
      if (receiveBufferSize > 0) {
        sendSocket.setReceiveBufferSize(receiveBufferSize);
      }
      sendSocket.connect(new InetSocketAddress(target.getHost(), target
          .getPort()));
      linkWithThreads(receiveSocket, sendSocket);
      if (log.isInfoEnabled()) {
        log.info("proxy connection {}, receiveBufferSize={}", sendSocket, sendSocket.getReceiveBufferSize());
      }
    }
    
    public void goOn() {
      responseThread.goOn();
      requestThread.goOn();
    }
    
    public void pause() {
      requestThread.pause();
      responseThread.pause();
    }
    
    public void close() throws Exception {
      synchronized (connections) {
        connections.remove(this);
      }
      receiveSocket.close();
      sendSocket.close();
    }
    
    public void halfClose() throws Exception {
      receiveSocket.close();
    }
    
    private void linkWithThreads(Socket source, Socket dest) {
      requestThread = new Pump("Request", source, dest);
      requestThread.start();
      responseThread = new Pump("Response", dest, source);
      responseThread.start();
    }
    
    public class Pump extends Thread {
      
      protected Socket src;
      private Socket destination;
      private AtomicReference<CountDownLatch> pause = new AtomicReference<CountDownLatch>();
      
      public Pump(String kind, Socket source, Socket dest) {
        super("SocketProxy-"+kind+"-" + source.getPort() + ":"
            + dest.getPort());
        src = source;
        destination = dest;
        pause.set(new CountDownLatch(0));
      }
      
      public void pause() {
        pause.set(new CountDownLatch(1));
      }
      
      public void goOn() {
        pause.get().countDown();
      }
      
      public void run() {
        byte[] buf = new byte[1024];

        try {
          src.setSoTimeout(PUMP_SOCKET_TIMEOUT_MS);
        } catch (SocketException e) {
          if (e.getMessage().equals("Socket is closed")) {
            log.warn("Failed to set socket timeout on {} due to: ", src, e);
            return;
          }
          log.error("Failed to set socket timeout on {} due to ",src, e);
          throw new RuntimeException(e);
        }

        InputStream in = null;
        OutputStream out = null;
        try {
          in = src.getInputStream();
          out = destination.getOutputStream();
          while (true) {
            int len = -1;
            try {
              len = in.read(buf);
            } catch (SocketTimeoutException ste) {
              log.warn("Error when reading from {}", src, ste);
            }

            if (len == -1) {
              log.debug("read eof from: {}", src);
              break;
            }
            pause.get().await();
            if (len > 0)
              out.write(buf, 0, len);
          }
        } catch (Exception e) {
          if (log.isDebugEnabled()) {
            log.debug("read/write failed, reason: {}", e.getLocalizedMessage());
          }
          try {
            if (!receiveSocket.isClosed()) {
              // for halfClose, on read/write failure if we close the
              // remote end will see a close at the same time.
              close();
            }
          } catch (Exception ignore) {}
        } finally {
          if (in != null) {
            try {
              in.close();
            } catch (Exception exc) {
              log.debug("Error when closing InputStream on socket: {}", src, exc);
            }
          }
          if (out != null) {
            try {
              out.close();
            } catch (Exception exc) {
              log.debug("{} when closing OutputStream on socket: {}", exc, destination);
            }
          }
        }
      }
    }
  }
  
  public class Acceptor implements Runnable {
    
    private ServerSocket socket;
    private URI target;
    private AtomicReference<CountDownLatch> pause = new AtomicReference<CountDownLatch>();
    
    public Acceptor(ServerSocket serverSocket, URI uri) {
      socket = serverSocket;
      target = uri;
      pause.set(new CountDownLatch(0));
      try {
        socket.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
      } catch (SocketException e) {
        e.printStackTrace();
      }
    }
    
    public void pause() {
      pause.set(new CountDownLatch(1));
    }
    
    public void goOn() {
      pause.get().countDown();
    }
    
    public void run() {
      try {
        while (!socket.isClosed()) {
          pause.get().await();
          try {
            Socket source = socket.accept();
            pause.get().await();
            if (receiveBufferSize > 0) {
              source.setReceiveBufferSize(receiveBufferSize);
            }
            if (log.isInfoEnabled()) {
              log.info("accepted {}, receiveBufferSize: {}", source, source.getReceiveBufferSize());
            }
            synchronized (connections) {
              connections.add(new Bridge(source, target));
            }
          } catch (SocketTimeoutException expected) {}
        }
      } catch (Exception e) {
        if (log.isDebugEnabled()) {
          log.debug("acceptor: finished for reason: {}", e.getLocalizedMessage());
        }
      }
    }
    
    public void close() {
      try {
        socket.close();
        closed.countDown();
        goOn();
      } catch (IOException ignored) {}
    }
  }
  
}
