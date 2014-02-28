package org.apache.lucene.server;

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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.server.handlers.*;
import org.apache.lucene.server.http.HttpStaticFileServerHandler;
import org.apache.lucene.server.params.*;
import org.apache.lucene.server.params.PolyType.PolyEntry;
import org.apache.lucene.util.NamedThreadFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
//import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyleIdent;
import net.minidev.json.parser.ContainerFactory;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

//import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
//import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
//import static org.jboss.netty.handler.codec.http.HttpMethod.*;


// nocommit move under http

/** Main entry point for the HTTP server. */
public class Server {

  final GlobalState globalState;

  final SimpleChannelUpstreamHandler staticFileHandler;

  /** The actual port server bound to (only interesting if
   *  you passed port=0 to let the OS assign one). */
  public int actualPort;

  /** Handles the incoming request. */
  private class Dispatcher extends SimpleChannelUpstreamHandler {

    private Writer pipedWriter;
    private Throwable[] pipedExc;
    private Thread pipeThread;
    private boolean firstMessage = true;
    private final String[] pipedResult = new String[1];
    private ByteArrayOutputStream chunkedRequestBytes;
    private String command;
    private Handler handler;
    private boolean doKeepAlive = true;
    private Map<String,List<String>> params;

    private byte[] processRequest(String request) throws Exception {
      //System.out.println("request: " + request + " this=" + this);
      JSONObject requestData;
      if (request != null) {
        Object o = null;
        try {
          //System.out.println("request " + request);
          o = new JSONParser(JSONParser.MODE_STRICTEST).parse(request, ContainerFactory.FACTORY_SIMPLE);
        } catch (ParseException pe) {
          IllegalArgumentException iae = new IllegalArgumentException("could not parse HTTP request data as JSON");
          iae.initCause(pe);
          throw iae;
        }
        if (!(o instanceof JSONObject)) {
          throw new IllegalArgumentException("HTTP request data must be a JSON struct { .. }");
        }
        requestData = (JSONObject) o;
      } else {
        requestData = new JSONObject();
      }

      Request r = new Request(null, command, requestData, handler.getType());
      IndexState state;
      if (handler.requiresIndexName) {
        String indexName = r.getString("indexName");
        state = globalState.get(indexName);
      } else {
        state = null;
      }

      FinishRequest finish;
      try {
        for(PreHandle h : handler.preHandlers) {
          h.invoke(r);
        }
        // TODO: for "compute intensive" (eg search)
        // handlers, we should use a separate executor?  And
        // we should more gracefully handle the "Too Busy"
        // case by accepting the connection, seeing backlog
        // is too much, and sending HTTP 500 back
        finish = handler.handle(state, r, params);
      } catch (RequestFailedException rfe) {
        String details = null;
        if (rfe.param != null) {

          // nocommit this seems to not help, ie if a
          // handler threw an exception on a specific
          // parameter, it means something went wrong w/
          // that param, and it's not (rarely?) helpful to
          // then list all the other valid params?

          /*
          if (rfe.request.getType() != null) {
            Param p = rfe.request.getType().params.get(rfe.param);
            if (p != null) {
              if (p.type instanceof StructType) {
                List<String> validParams = new ArrayList<String>(((StructType) p.type).params.keySet());
                Collections.sort(validParams);
                details = "valid params are: " + validParams.toString();
              } else if (p.type instanceof ListType && (((ListType) p.type).subType instanceof StructType)) {
                List<String> validParams = new ArrayList<String>(((StructType) ((ListType) p.type).subType).params.keySet());
                Collections.sort(validParams);
                details = "each element in the array may have these params: " + validParams.toString();
              }
            }
          }
          */
        } else {
          List<String> validParams = new ArrayList<String>(rfe.request.getType().params.keySet());
          Collections.sort(validParams);
          details = "valid params are: " + validParams.toString();
        }

        if (details != null) {
          rfe = new RequestFailedException(rfe, details);
        }

        throw rfe;
      }

      // We remove params as they are accessed, so if
      // anything is left it means it wasn't used:
      if (Request.anythingLeft(requestData)) {
        assert request != null;
        JSONObject fullRequest;
        try {
          fullRequest = (JSONObject) new JSONParser(JSONParser.MODE_STRICTEST).parse(request, ContainerFactory.FACTORY_SIMPLE);          
        } catch (ParseException pe) {
          // The request parsed originally...:
          assert false;

          // Dead code but compiler disagrees:
          fullRequest = null;
        }

        // Pretty print the leftover (unhandled) params:
        String pretty = requestData.toJSONString(new JSONStyleIdent());
        String s = "unrecognized parameters:\n" + pretty;
        String details = findFirstWrongParam(handler.getType(), fullRequest, requestData, new ArrayList<String>());
        if (details != null) {
          s += "\n\n" + details;
        }
        throw new IllegalArgumentException(s);
      }

      String response = finish.finish();
      //System.out.println("  response: " + response);

      return response.getBytes("UTF-8");
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String details) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
      response.setContent(ChannelBuffers.copiedBuffer(
                                                      "Failure: " + status.toString() + "\r\nDetails: " + details,
                                                      CharsetUtil.UTF_8));
      ChannelFuture later = ctx.getChannel().write(response);
      if (!doKeepAlive) {
        // Close the connection as soon as the error message is sent.
        later.addListener(ChannelFutureListener.CLOSE);
      }
    }

    /** Serves a static file from a plugin */
    private void handlePlugin(ChannelHandlerContext ctx, HttpRequest request) throws Exception {
      String uri = request.getUri();
      int idx = uri.indexOf('/', 9);
      if (idx == -1) {
        sendError(ctx, HttpResponseStatus.BAD_REQUEST, "URL should be /plugin/name/...");
        return;
      }
          
      String pluginName = uri.substring(9, idx);
      File pluginsDir = new File(globalState.stateDir, "plugins");
      File pluginDir = new File(pluginsDir, pluginName);
      if (!pluginDir.exists()) {
        sendError(ctx, HttpResponseStatus.BAD_REQUEST, "plugin \"" + pluginName + "\" does not exist");
      }

      staticFileHandler.messageReceived(ctx, new UpstreamMessageEvent(ctx.getChannel(),
                                                                      new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                                                             HttpMethod.GET,
                                                                                             pluginName + "/site/" + uri.substring(idx+1)),
                                                                      null));
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

      byte[] responseBytes = null;

      if (firstMessage) {
        firstMessage = false;
        HttpRequest request = (HttpRequest) e.getMessage();

        // nocommit do we need to check this per-chunk?
        String s = request.getHeader("Connection");
        if (s != null && s.toLowerCase(Locale.ROOT).equals(HttpHeaders.Values.CLOSE)) {
          doKeepAlive = false;
        }

        String uri = request.getUri();
        if (uri.startsWith("/plugin")) {
          handlePlugin(ctx, request);
          return;
        }

        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        command = decoder.getPath().substring(1);

        params = decoder.getParameters();

        if (command.equals("doc")) {
          String html = globalState.docHandler.handle(params, globalState.getHandlers());
          responseBytes = html.getBytes("UTF-8");
          Channel ch = e.getChannel();
          HttpResponse r = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          r.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text-html;charset=utf-8");
          r.setHeader(HttpHeaders.Names.CONTENT_LENGTH, ""+responseBytes.length);
          r.setContent(ChannelBuffers.copiedBuffer(responseBytes));
          ch.write(r);
          return;
        }

        // Dispatch by command:
        try {
          handler = globalState.getHandler(command);
        } catch (IllegalArgumentException iae) {
          throw new IllegalArgumentException("unrecognized method \"" + command + "\"");
        }

        if (handler.doStream()) {
          //System.out.println("DO STREAM");
          if (!request.isChunked()) {
            //System.out.println("  not chunked");
            // nocommit must get encoding, not assume UTF-8:

            // Streaming handler but request wasn't chunked;
            // just read all bytes into StringReader:
            String result = handler.handleStreamed(new StringReader(request.getContent().toString(CharsetUtil.UTF_8)), params);
            responseBytes = result.getBytes("UTF-8");
          } else {
            //System.out.println("  is chunked");
            final Pipe p = new Pipe(1.0f);
            pipedWriter = p.getWriter();
            final Reader pipedReader = p.getReader();
            pipedExc = new Throwable[1];
            pipeThread = new Thread() {
                @Override
                public void run() {
                  try {
                    pipedResult[0] = handler.handleStreamed(pipedReader, params);
                  } catch (Throwable t) {
                    // nocommit use Future/call so exc is forwarded
                    pipedExc[0] = t;
                    p.close();
                    //throw new RuntimeException(t);
                  }
                }
              };
            pipeThread.setName("Chunked HTTP");
            pipeThread.start();
          }
        } else if (request.isChunked()) {
          chunkedRequestBytes = new ByteArrayOutputStream();
        } else {

          String data;
          if (request.getMethod() == HttpMethod.POST) {
            /*
              System.out.println("BUFFER: " + request.getContent());
              if (command.equals("addDocument")) {
              byte[] buffer = new byte[10];
              while(true) {
              request.getContent().readBytes(buffer);
              System.out.println("here: " + buffer);
              }
              }
            */
            //ChannelBuffer cb = request.getContent();
            //System.out.println("cp cap: " + cb.capacity() + " readable=" + cb.readableBytes() + " chunked=" + request.isChunked());
            // nocommit must get encoding, not assume UTF-8:
            data = request.getContent().toString(CharsetUtil.UTF_8);
          } else {
            data = null;
          }

          responseBytes = processRequest(data);
        }
      } else if (chunkedRequestBytes != null) {
        if (pipedExc != null) {
          maybeThrowPipeExc();
        }
        HttpChunk chunk = (HttpChunk) e.getMessage();
        if (chunk.isLast()) {
          // nocommit must verify encoding is UTF-8:
          responseBytes = processRequest(chunkedRequestBytes.toString("UTF-8"));
        } else {
          // nocommit can we do single copy?
          ChannelBuffer cb = chunk.getContent();
          byte[] copy = new byte[cb.readableBytes()];
          cb.readBytes(copy);
          chunkedRequestBytes.write(copy);
        }
      } else {
        try {
          HttpChunk chunk = (HttpChunk) e.getMessage();
          if (chunk.isLast()) {
            pipedWriter.close();
            pipedWriter = null;
            pipeThread.join();
            pipeThread = null;
            responseBytes = pipedResult[0].getBytes("UTF-8");
            firstMessage = true;
          } else {
            // nocommit must get encoding, not assume UTF-8:
            ChannelBuffer cb = chunk.getContent();
            byte[] copy = new byte[cb.readableBytes()];
            cb.readBytes(copy);
            char[] arr = IndexState.utf8ToCharArray(copy, 0, copy.length);
            pipedWriter.write(arr, 0, arr.length);
            //System.out.println("write another " + arr.length);
          }
        } catch (Throwable t) {
          maybeThrowPipeExc();
          throw new RuntimeException(t);
        }
      }

      // sendError(ctx, METHOD_NOT_ALLOWED);
      // sendError(ctx, FORBIDDEN);
      // sendError(ctx, NOT_FOUND);
      if (responseBytes != null) {
        Channel ch = e.getChannel();
        if (ch.isConnected()) {
          HttpResponse r = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          r.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text-plain; charset=utf-8");
          r.setHeader(HttpHeaders.Names.CONTENT_LENGTH, ""+responseBytes.length);
          if (doKeepAlive) {
            r.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          }
          r.setContent(ChannelBuffers.copiedBuffer(responseBytes));
          ChannelFuture f = ch.write(r);
          if (!doKeepAlive) {
            f.addListener(ChannelFutureListener.CLOSE);
          } else {
            firstMessage = true;
          }
        }
      }

      super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      //System.out.println("SVR.exceptionCaught: " + e);
      //e.getCause().printStackTrace(System.out);

      Channel ch = e.getChannel();
      if (!ch.isConnected()) {
        return;
      }
      // Just send full stack trace back to client:
      Writer sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      if (e.getCause() instanceof RequestFailedException) {
        RequestFailedException rfe = (RequestFailedException) e.getCause();
        pw.write(rfe.path + ": " + rfe.reason);
        // TODO?
        //Throwable cause = rfe.getCause();
        //if (cause != null) {
        //pw.write("\n\nCaused by:\n\n" + cause);
        //}
      } else {
        e.getCause().printStackTrace(pw);
      }

      HttpResponse r = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
      //HttpResponse r = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      r.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text-plain; charset=utf-8");
      byte[] bytes = sw.toString().getBytes("UTF-8");
      r.setHeader(HttpHeaders.Names.CONTENT_LENGTH, "" + bytes.length);
      r.setContent(ChannelBuffers.copiedBuffer(bytes));
      try {
        ch.write(r).addListener(ChannelFutureListener.CLOSE);
      } catch (Throwable t) {
      }
    }

    private void maybeThrowPipeExc() throws Exception {
      Throwable t = pipedExc[0];
      if (t != null) {
        if (pipedWriter != null) {
          pipedWriter.close();
        }
        if (t instanceof Exception) {
          throw (Exception) t;
        } else if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else if (t instanceof Error) {
          throw (Error) t;
        } else {
          throw new RuntimeException(t);
        }
      }
    }
  }

  private static PolyEntry findPolyType(JSONObject fullRequest, StructType type) {
    for(Map.Entry<String,Param> param : type.params.entrySet()) {
      if (param.getValue().type instanceof PolyType) {
        Object v = fullRequest.get(param.getKey());
        if (v != null && v instanceof String) {
          PolyEntry polyEntry = ((PolyType) param.getValue().type).types.get((String) v);
          if (polyEntry != null) {
            return polyEntry;
          }
        }
      }
    }

    return null;
  }

  // TODO: should we find ALL wrong params?
  // nocommit improve this: it needs to recurse into arrays too
  private static String findFirstWrongParam(StructType type, JSONObject fullRequest, JSONObject r, List<String> path) {
    PolyEntry polyEntry = findPolyType(fullRequest, type);
    for(Map.Entry<String,Object> ent : r.entrySet()) {
      String param = ent.getKey();
      if (!type.params.containsKey(param) && !type.params.containsKey("*") && (polyEntry == null || !polyEntry.type.params.containsKey(param))) {

        List<String> validParams = null;
        String extra = "";
        if (polyEntry != null) {
          validParams = new ArrayList<String>(polyEntry.type.params.keySet());
          extra = " for class=" + polyEntry.name;
        } else {
        // No PolyType found:
          validParams = new ArrayList<String>(type.params.keySet());
        }
        Collections.sort(validParams);
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<path.size();i++) {
          if (i > 0) {
            sb.append(" > ");
          }
          sb.append(path.get(i));
        }

        if (sb.length() != 0) {
          sb.append(" > ");
        }
        sb.append(param);
        return "param " + sb.toString() + " is unrecognized; valid params" + extra + " are: " + validParams.toString();
      }
    }
      
    // Recurse:
    for(Map.Entry<String,Object> ent : r.entrySet()) {
      Param param = type.params.get(ent.getKey());
      if (param == null && polyEntry != null) {
        param = polyEntry.type.params.get(ent.getKey());
      }
      if (param == null) {
        param = type.params.get("*");
      }
      // nocommit go into array, poly too
      // nocommit handle case where we expected object but
      // didnt' get json object
      if (param.type instanceof StructType && ent.getValue() instanceof JSONObject) {
        path.add(param.name);
        String details = findFirstWrongParam((StructType) param.type, (JSONObject) fullRequest.get(ent.getKey()), (JSONObject) ent.getValue(), path);
        if (details != null) {
          return details;
        }
        path.remove(path.size()-1);
      }
    }

    return null;
  }

  private static void usage() {
    System.out.println("\nUsage: java -cp <stuff> org.apache.lucene.server.Server [-port port] [-maxHTTPThreadCount count] [-stateDir /path/to/dir]\n\n");
  }

  /** Sole constructor. */
  public Server(File globalStateDir) throws IOException {
    globalState = new GlobalState(globalStateDir);
    staticFileHandler = new HttpStaticFileServerHandler(new File(globalState.stateDir, "plugins"));
  }

  /** Runs the server. */
  public void run(int port, int maxHTTPThreadCount, CountDownLatch ready) throws Exception {

    globalState.loadIndexNames();

    // nocommit use fixed thread pools, so we don't cycle
    // threads through Lucene's CloseableThreadLocals!
    ExecutorService bossThreads = new ThreadPoolExecutor(0, maxHTTPThreadCount, 60L,
                                                         TimeUnit.SECONDS,
                                                         new SynchronousQueue<Runnable>(),
                                                         new NamedThreadFactory("LuceneServer-boss"));
    ExecutorService workerThreads = new ThreadPoolExecutor(0, maxHTTPThreadCount, 60L,
                                                           TimeUnit.SECONDS,
                                                           new SynchronousQueue<Runnable>(),
                                                           new
                                                         NamedThreadFactory("LuceneServer-worker"));
    
    //ExecutorService bossThreads = Executors.newCachedThreadPool();
    //ExecutorService workerThreads = Executors.newCachedThreadPool();

    ServerBootstrap bootstrap = null;
    try {

      ChannelFactory factory = new NioServerSocketChannelFactory(bossThreads, workerThreads, maxHTTPThreadCount);
      //ChannelFactory factory = new NioServerSocketChannelFactory(bossThreads, workerThreads);
      bootstrap = new ServerBootstrap(factory);

      bootstrap.setOption("reuseAddress", true);
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
          @Override
          public ChannelPipeline getPipeline() {
            ChannelPipeline p = Channels.pipeline();
            p.addLast("decoder", new HttpRequestDecoder());
            p.addLast("encoder", new HttpResponseEncoder());
            p.addLast("deflater", new HttpContentCompressor(1));
            // nocommit deflater (HttpContentCompressor)?
            p.addLast("handler", new Dispatcher());
            return p;
          }
        });

      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setOption("child.keepAlive", true);
        
      // Bind and start to accept incoming connections.
      Channel sc = bootstrap.bind(new InetSocketAddress(port));
      this.actualPort = ((InetSocketAddress) sc.getLocalAddress()).getPort();

      globalState.addHandler("addDocument", new AddDocumentHandler(globalState));
      globalState.addHandler("addDocuments", new AddDocumentsHandler(globalState));
      globalState.addHandler("analyze", new AnalysisHandler(globalState));
      globalState.addHandler("buildSuggest", new BuildSuggestHandler(globalState));
      globalState.addHandler("bulkAddDocument", new BulkAddDocumentHandler(globalState));
      globalState.addHandler("bulkAddDocuments", new BulkAddDocumentsHandler(globalState));
      globalState.addHandler("bulkUpdateDocument", new BulkUpdateDocumentHandler(globalState));
      globalState.addHandler("bulkUpdateDocuments", new BulkUpdateDocumentsHandler(globalState));
      globalState.addHandler("commit", new CommitHandler(globalState));
      globalState.addHandler("createIndex", new CreateIndexHandler(globalState));
      globalState.addHandler("createSnapshot", new CreateSnapshotHandler(globalState));
      globalState.addHandler("deleteAllDocuments", new DeleteAllDocumentsHandler(globalState));
      globalState.addHandler("deleteIndex", new DeleteIndexHandler(globalState));
      globalState.addHandler("deleteDocuments", new DeleteDocumentsHandler(globalState));
      globalState.addHandler("liveSettings", new LiveSettingsHandler(globalState));
      globalState.addHandler("liveValues", new LiveValuesHandler(globalState));
      globalState.addHandler("registerFields", new RegisterFieldHandler(globalState));
      globalState.addHandler("releaseSnapshot", new ReleaseSnapshotHandler(globalState));
      globalState.addHandler("search", new SearchHandler(globalState));
      globalState.addHandler("settings", new SettingsHandler(globalState));
      globalState.addHandler("shutdown", new ShutdownHandler(globalState));
      globalState.addHandler("startIndex", new StartIndexHandler(globalState));
      globalState.addHandler("stats", new StatsHandler(globalState));
      globalState.addHandler("stopIndex", new StopIndexHandler(globalState));
      globalState.addHandler("suggestLookup", new SuggestLookupHandler(globalState));
      globalState.addHandler("updateSuggest", new UpdateSuggestHandler(globalState));
      globalState.addHandler("updateDocument", new UpdateDocumentHandler(globalState));

      globalState.loadPlugins();

      System.out.println("SVR: listening on port " + actualPort + ".");

      // Notify caller server is started:
      ready.countDown();

      // Await shutdown:
      globalState.shutdownNow.await();

      // Close everything:
      sc.close().awaitUninterruptibly();

      globalState.close();

    } finally {
      if (bootstrap != null) {
        bootstrap.releaseExternalResources();
      }
      bossThreads.shutdown();
      workerThreads.shutdown();
    }
  }

  /** Command-line entry. */
  public static void main(String[] args) throws Exception {
    File stateDir = null;

    int port = 4000;
    int maxHTTPThreadCount = 2*Runtime.getRuntime().availableProcessors();
    for(int i=0;i<args.length;i++) {
      if (args[i].equals("-port")) {
        if (args.length == i+1) {
          throw new IllegalArgumentException("no value specified after -port");
        }
        port = Integer.parseInt(args[i+1]);
        i++;
      } else if (args[i].equals("-maxHTTPThreadCount")) {
        if (args.length == i+1) {
          throw new IllegalArgumentException("no value specified after -maxHTTPThreadCount");
        }
        maxHTTPThreadCount = Integer.parseInt(args[i+1]);
        i++;
      } else if (args[i].equals("-stateDir")) {
        if (args.length == i+1) {
          throw new IllegalArgumentException("no value specified after -stateDir");
        }
        stateDir = new File(args[i+1]);
        i++;
      } else {
        usage();
        System.exit(-1);
      }
    }

    new Server(stateDir).run(port, maxHTTPThreadCount, new CountDownLatch(1));
  }
}
