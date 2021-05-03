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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.TestRuleIgnoreTestSuites;
import org.apache.lucene.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;

/** Child process with silly naive TCP socket server to handle
 *  between-node commands, launched for each node  by TestNRTReplication. */
@SuppressCodecs({"MockRandom", "Direct", "SimpleText"})
@SuppressSysoutChecks(bugUrl = "Stuff gets printed, important stuff for debugging a failure")
@SuppressForbidden(reason = "We need Unsafe to actually crush :-)")
public class TestSimpleServer extends LuceneTestCase {

  final static Set<Thread> clientThreads = Collections.synchronizedSet(new HashSet<>());
  final static AtomicBoolean stop = new AtomicBoolean();

  /** Handles one client connection */
  private static class ClientHandler extends Thread {

    // We hold this just so we can close it to exit the process:
    private final ServerSocket ss;
    private final Socket socket;
    private final Node node;
    private final int bufferSize;

    public ClientHandler(ServerSocket ss, Node node, Socket socket) {
      this.ss = ss;
      this.node = node;
      this.socket = socket;
      this.bufferSize = TestUtil.nextInt(random(), 128, 65536);
      if (Node.VERBOSE_CONNECTIONS) {
        node.message("new connection socket=" + socket);
      }
    }

    @Override
    public void run() {
      boolean success = false;
      try {
        //node.message("using stream buffer size=" + bufferSize);
        InputStream is = new BufferedInputStream(socket.getInputStream(), bufferSize);
        DataInput in = new InputStreamDataInput(is);
        BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream(), bufferSize);
        DataOutput out = new OutputStreamDataOutput(bos);

        if (node instanceof SimplePrimaryNode) {
          ((SimplePrimaryNode) node).handleOneConnection(random(), ss, stop, is, socket, in, out, bos);
        } else {
          ((SimpleReplicaNode) node).handleOneConnection(ss, stop, is, socket, in, out, bos);
        }

        bos.flush();
        if (Node.VERBOSE_CONNECTIONS) {
          node.message("bos.flush done");
        }

        success = true;
      } catch (Throwable t) {
        if (t instanceof SocketException == false && t instanceof NodeCommunicationException == false) {
          node.message("unexpected exception handling client connection; now failing test:");
          t.printStackTrace(System.out);
          IOUtils.closeWhileHandlingException(ss);
          // Test should fail with this:
          throw new RuntimeException(t);
        } else {
          node.message("exception handling client connection; ignoring:");
          t.printStackTrace(System.out);
        }
      } finally {
        if (success) {
          try {
            IOUtils.close(socket);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        } else {
          IOUtils.closeWhileHandlingException(socket);
        }
      }
      if (Node.VERBOSE_CONNECTIONS) {
        node.message("socket.close done");
      }
    }
  }

  /**
   * currently, this only works/tested on Sun and IBM.
   */

  // poached from TestIndexWriterOnJRECrash ... should we factor out to TestUtil?  seems dangerous to give it such "publicity"?
  private static void crashJRE() {
    final String vendor = Constants.JAVA_VENDOR;
    final boolean supportsUnsafeNpeDereference = 
        vendor.startsWith("Oracle") || 
        vendor.startsWith("Sun") || 
        vendor.startsWith("Apple");

    try {
      if (supportsUnsafeNpeDereference) {
        try {
          Class<?> clazz = Class.forName("sun.misc.Unsafe");
          java.lang.reflect.Field field = clazz.getDeclaredField("theUnsafe");
          field.setAccessible(true);
          Object o = field.get(null);
          Method m = clazz.getMethod("putAddress", long.class, long.class);
          m.invoke(o, 0L, 0L);
        } catch (Throwable e) {
          System.out.println("Couldn't kill the JVM via Unsafe.");
          e.printStackTrace(System.out); 
        }
      }

      // Fallback attempt to Runtime.halt();
      Runtime.getRuntime().halt(-1);
    } catch (Exception e) {
      System.out.println("Couldn't kill the JVM.");
      e.printStackTrace(System.out); 
    }

    // We couldn't get the JVM to crash for some reason.
    throw new RuntimeException("JVM refuses to die!");
  }

  static void writeFilesMetaData(DataOutput out, Map<String,FileMetaData> files) throws IOException {
    out.writeVInt(files.size());
    for(Map.Entry<String,FileMetaData> ent : files.entrySet()) {
      out.writeString(ent.getKey());

      FileMetaData fmd = ent.getValue();
      out.writeVLong(fmd.length);
      out.writeVLong(fmd.checksum);
      out.writeVInt(fmd.header.length);
      out.writeBytes(fmd.header, 0, fmd.header.length);
      out.writeVInt(fmd.footer.length);
      out.writeBytes(fmd.footer, 0, fmd.footer.length);
    }
  }

  static Map<String,FileMetaData> readFilesMetaData(DataInput in) throws IOException {
    int fileCount = in.readVInt();
    //System.out.println("readFilesMetaData: fileCount=" + fileCount);
    Map<String,FileMetaData> files = new HashMap<>();
    for(int i=0;i<fileCount;i++) {
      String fileName = in.readString();
      //System.out.println("readFilesMetaData: fileName=" + fileName);
      long length = in.readVLong();
      long checksum = in.readVLong();
      byte[] header = new byte[in.readVInt()];
      in.readBytes(header, 0, header.length);
      byte[] footer = new byte[in.readVInt()];
      in.readBytes(footer, 0, footer.length);
      files.put(fileName, new FileMetaData(header, footer, length, checksum));
    }
    return files;
  }

  /** Pulls CopyState off the wire */
  static CopyState readCopyState(DataInput in) throws IOException {

    // Decode a new CopyState
    byte[] infosBytes = new byte[in.readVInt()];
    in.readBytes(infosBytes, 0, infosBytes.length);

    long gen = in.readVLong();
    long version = in.readVLong();
    Map<String,FileMetaData> files = readFilesMetaData(in);

    int count = in.readVInt();
    Set<String> completedMergeFiles = new HashSet<>();
    for(int i=0;i<count;i++) {
      completedMergeFiles.add(in.readString());
    }
    long primaryGen = in.readVLong();

    return new CopyState(files, version, gen, infosBytes, completedMergeFiles, primaryGen, null);
  }

  @BeforeClass
  public static void ensureNested() {
    Assume.assumeTrue(TestRuleIgnoreTestSuites.isRunningNested());
  }

  @SuppressWarnings("try")
  public void test() throws Exception {

    int id = Integer.parseInt(System.getProperty("tests.nrtreplication.nodeid"));
    Thread.currentThread().setName("main child " + id);
    Path indexPath = Paths.get(System.getProperty("tests.nrtreplication.indexpath"));
    boolean isPrimary = System.getProperty("tests.nrtreplication.isPrimary") != null;
    int primaryTCPPort;
    long forcePrimaryVersion;
    if (isPrimary == false) {
      forcePrimaryVersion = -1;
      primaryTCPPort = Integer.parseInt(System.getProperty("tests.nrtreplication.primaryTCPPort"));
    } else {
      primaryTCPPort = -1;
      forcePrimaryVersion = Long.parseLong(System.getProperty("tests.nrtreplication.forcePrimaryVersion"));
    }
    long primaryGen = Long.parseLong(System.getProperty("tests.nrtreplication.primaryGen"));
    Node.globalStartNS = Long.parseLong(System.getProperty("tests.nrtreplication.startNS"));

    boolean doRandomCrash = "true".equals(System.getProperty("tests.nrtreplication.doRandomCrash"));
    boolean doRandomClose = "true".equals(System.getProperty("tests.nrtreplication.doRandomClose"));
    boolean doFlipBitsDuringCopy = "true".equals(System.getProperty("tests.nrtreplication.doFlipBitsDuringCopy"));
    boolean doCheckIndexOnClose = "true".equals(System.getProperty("tests.nrtreplication.checkonclose"));

    // Create server socket that we listen for incoming requests on:
    try (final ServerSocket ss = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {

      int tcpPort = ((InetSocketAddress) ss.getLocalSocketAddress()).getPort();
      System.out.println("\nPORT: " + tcpPort);
      final Node node;
      if (isPrimary) {
        node = new SimplePrimaryNode(random(), indexPath, id, tcpPort, primaryGen, forcePrimaryVersion, null, doFlipBitsDuringCopy, doCheckIndexOnClose);
        System.out.println("\nCOMMIT VERSION: " + ((PrimaryNode) node).getLastCommitVersion());
      } else {
        try {
          node = new SimpleReplicaNode(random(), id, tcpPort, indexPath, primaryGen, primaryTCPPort, null, doCheckIndexOnClose);
        } catch (RuntimeException re) {
          if (re.getMessage().startsWith("replica cannot start")) {
            // this is "OK": it means MDW's refusal to delete a segments_N commit point means we cannot start:
            assumeTrue(re.getMessage(), false);
          }
          throw re;
        }
      }
      System.out.println("\nINFOS VERSION: " + node.getCurrentSearchingVersion());

      if (doRandomClose || doRandomCrash) {
        final int waitForMS;
        if (isPrimary) {
          waitForMS = TestUtil.nextInt(random(), 20000, 60000);
        } else {
          waitForMS = TestUtil.nextInt(random(), 5000, 60000);
        }

        boolean doClose;
        if (doRandomCrash == false) {
          doClose = true;
        } else if (doRandomClose) {
          doClose = random().nextBoolean();
        } else {
          doClose = false;
        }

        if (doClose) {
          node.message("top: will close after " + (waitForMS/1000.0) + " seconds");
        } else {
          node.message("top: will crash after " + (waitForMS/1000.0) + " seconds");
        }

        Thread t = new Thread() {
            @Override
            public void run() {
              long endTime = System.nanoTime() + waitForMS*1000000L;
              while (System.nanoTime() < endTime) {
                try {
                  Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                if (stop.get()) {
                  break;
                }
              }

              if (stop.get() == false) {
                if (doClose) {
                  try {
                    node.message("top: now force close server socket after " + (waitForMS/1000.0) + " seconds");
                    node.state = "top-closing";
                    ss.close();
                  } catch (IOException ioe) {     
                    throw new RuntimeException(ioe);
                  }
                } else {        
                  node.message("top: now crash JVM after " + (waitForMS/1000.0) + " seconds");
                  crashJRE();
                }
              }
            }
          };

        if (isPrimary) {
          t.setName("crasher P" + id);
        } else {
          t.setName("crasher R" + id);
        }

        // So that if node exits naturally, this thread won't prevent process exit:
        t.setDaemon(true);
        t.start();
      }
      System.out.println("\nNODE STARTED");

      //List<Thread> clientThreads = new ArrayList<>();

      // Naive thread-per-connection server:
      while (true) {
        Socket socket;
        try {
          socket = ss.accept();
        } catch (SocketException se) {
          // when ClientHandler closes our ss we will hit this
          node.message("top: server socket exc; now exit");
          break;
        }
        Thread thread = new ClientHandler(ss, node, socket);
        thread.setDaemon(true);
        thread.start();

        clientThreads.add(thread);

        // Prune finished client threads:
        Iterator<Thread> it = clientThreads.iterator();
        while (it.hasNext()) {
          Thread t = it.next();
          if (t.isAlive() == false) {
            it.remove();
          }
        }
        //node.message(clientThreads.size() + " client threads are still alive");
      }

      stop.set(true);

      // Make sure all client threads are done, else we get annoying (yet ultimately "harmless") messages about threads still running /
      // lingering for them to finish from the child processes:
      for(Thread clientThread : clientThreads) {
        node.message("top: join clientThread=" + clientThread);
        clientThread.join();
        node.message("top: done join clientThread=" + clientThread);
      }
      node.message("done join all client threads; now close node");
      node.close();
    }
  }
}
