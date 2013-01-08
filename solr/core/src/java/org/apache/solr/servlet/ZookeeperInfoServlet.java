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

package org.apache.solr.servlet;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.noggit.CharArr;
import org.apache.noggit.JSONWriter;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.FastWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Zookeeper Info
 *
 * @since solr 4.0
 */
public final class ZookeeperInfoServlet extends HttpServlet {
  static final Logger log = LoggerFactory.getLogger(ZookeeperInfoServlet.class);
  
  @Override
  public void init() {
  }

  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws ServletException,IOException {
    // This attribute is set by the SolrDispatchFilter
    CoreContainer cores = (CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");
    if (cores == null) {
      throw new ServletException("Missing request attribute org.apache.solr.CoreContainer.");
    }
    
    final SolrParams params;
    try {
      params = SolrRequestParsers.DEFAULT.parse(null, request.getServletPath(), request).getParams();
    } catch (Exception e) {
      int code=500;
      if (e instanceof SolrException) {
        code = Math.min(599, Math.max(100, ((SolrException)e).code()));
      }
      response.sendError(code, e.toString());
      return;
    }

    String path = params.get("path");
    String addr = params.get("addr");

    if (addr != null && addr.length() == 0) {
      addr = null;
    }

    String detailS = params.get("detail");
    boolean detail = detailS != null && detailS.equals("true");

    String dumpS = params.get("dump");
    boolean dump = dumpS != null && dumpS.equals("true");

    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");

    Writer out = new FastWriter(new OutputStreamWriter(response.getOutputStream(), IOUtils.CHARSET_UTF_8));

    ZKPrinter printer = new ZKPrinter(response, out, cores.getZkController(), addr);
    printer.detail = detail;
    printer.dump = dump;

    try {
      printer.print(path);
    } finally {
      printer.close();
    }
    
    out.flush();
  }

  @Override
  public void doPost(HttpServletRequest request,
                     HttpServletResponse response)
      throws ServletException,IOException {
    doGet(request, response);
  }


  //--------------------------------------------------------------------------------------
  //
  //--------------------------------------------------------------------------------------

  static class ZKPrinter {
    static boolean FULLPATH_DEFAULT = false;

    boolean indent = true;
    boolean fullpath = FULLPATH_DEFAULT;
    boolean detail = false;
    boolean dump = false;

    String addr; // the address passed to us
    String keeperAddr; // the address we're connected to

    boolean doClose;  // close the client after done if we opened it

    final HttpServletResponse response;
    final Writer out;
    SolrZkClient zkClient;

    int level;
    int maxData = 95;

    public ZKPrinter(HttpServletResponse response, Writer out, ZkController controller, String addr) throws IOException {
      this.response = response;
      this.out = out;
      this.addr = addr;

      if (addr == null) {
        if (controller != null) {
          // this core is zk enabled
          keeperAddr = controller.getZkServerAddress();
          zkClient = controller.getZkClient();
          if (zkClient != null && zkClient.isConnected()) {
            return;
          } else {
            // try a different client with this address
            addr = keeperAddr;
          }
        }
      }

      keeperAddr = addr;
      if (addr == null) {
        writeError(404, "Zookeeper is not configured for this Solr Core. Please try connecting to an alternate zookeeper address.");
        return;
      }

      try {
        zkClient = new SolrZkClient(addr, 10000);
        doClose = true;
      } catch (Exception e) {
        writeError(503, "Could not connect to zookeeper at '" + addr + "'\"");
        zkClient = null;
        return;
      }

    }

    public void close() {
      if (doClose) {
        zkClient.close();
      }
    }

    // main entry point
    void print(String path) throws IOException {
      if (zkClient == null) {
        return;
      }

      // normalize path
      if (path == null) {
        path = "/";
      } else {
        path.trim();
        if (path.length() == 0) {
          path = "/";
        }
      }

      if (path.endsWith("/") && path.length() > 1) {
        path = path.substring(0, path.length() - 1);
      }

      int idx = path.lastIndexOf('/');
      String parent = idx >= 0 ? path.substring(0, idx) : path;
      if (parent.length() == 0) {
        parent = "/";
      }

      CharArr chars = new CharArr();
      JSONWriter json = new JSONWriter(chars, 2);
      json.startObject();

      if (detail) {
        if (!printZnode(json, path)) {
          return;
        }
        json.writeValueSeparator();
      }

      json.writeString("tree");
      json.writeNameSeparator();
      json.startArray();
      if (!printTree(json, path)) {
        return; // there was an error
      }
      json.endArray();
      json.endObject();
      out.write(chars.toString());
    }

    void writeError(int code, String msg) throws IOException {
      response.setStatus(code);

      CharArr chars = new CharArr();
      JSONWriter w = new JSONWriter(chars, 2);
      w.startObject();
      w.indent();
      w.writeString("status");
      w.writeNameSeparator();
      w.write(code);
      w.writeValueSeparator();
      w.indent();
      w.writeString("error");
      w.writeNameSeparator();
      w.writeString(msg);
      w.endObject();

      out.write(chars.toString());
    }


    boolean printTree(JSONWriter json, String path) throws IOException {
      String label = path;
      if (!fullpath) {
        int idx = path.lastIndexOf('/');
        label = idx > 0 ? path.substring(idx + 1) : path;
      }
      json.startObject();
      //writeKeyValue(json, "data", label, true );
      json.writeString("data");
      json.writeNameSeparator();

      json.startObject();
      writeKeyValue(json, "title", label, true);
      json.writeValueSeparator();
      json.writeString("attr");
      json.writeNameSeparator();
      json.startObject();
      writeKeyValue(json, "href", "zookeeper?detail=true&path=" + URLEncoder.encode(path, "UTF-8"), true);
      json.endObject();
      json.endObject();

      Stat stat = new Stat();
      try {
        // Trickily, the call to zkClient.getData fills in the stat variable
        byte[] data = zkClient.getData(path, null, stat, true);

        if (stat.getEphemeralOwner() != 0) {
          writeKeyValue(json, "ephemeral", true, false);
          writeKeyValue(json, "version", stat.getVersion(), false);
        }

        if (dump) {
          json.writeValueSeparator();
          printZnode(json, path);
        }

        /*
        if (stat.getNumChildren() != 0)
        {
          writeKeyValue(json, "children_count",  stat.getNumChildren(), false );
          out.println(", \"children_count\" : \"" + stat.getNumChildren() + "\"");
        }
        */

        //if (stat.getDataLength() != 0)
        if (data != null) {
          String str = new BytesRef(data).utf8ToString();
          //?? writeKeyValue(json, "content", str, false );
          // Does nothing now, but on the assumption this will be used later we'll leave it in. If it comes out
          // the catches below need to be restructured.
        }
      } catch (IllegalArgumentException e) {
        // path doesn't exist (must have been removed)
        writeKeyValue(json, "warning", "(path gone)", false);
      } catch (KeeperException e) {
        writeKeyValue(json, "warning", e.toString(), false);
        log.warn("Keeper Exception", e);
      } catch (InterruptedException e) {
        writeKeyValue(json, "warning", e.toString(), false);
        log.warn("InterruptedException", e);
      }

      if (stat.getNumChildren() > 0) {
        json.writeValueSeparator();
        if (indent) {
          json.indent();
        }
        json.writeString("children");
        json.writeNameSeparator();
        json.startArray();

        try {
          List<String> children = zkClient.getChildren(path, null, true);
          java.util.Collections.sort(children);

          boolean first = true;
          for (String child : children) {
            if (!first) {
              json.writeValueSeparator();
            }

            String childPath = path + (path.endsWith("/") ? "" : "/") + child;
            if (!printTree(json, childPath)) {
              return false;
            }
            first = false;
          }
        } catch (KeeperException e) {
          writeError(500, e.toString());
          return false;
        } catch (InterruptedException e) {
          writeError(500, e.toString());
          return false;
        } catch (IllegalArgumentException e) {
          // path doesn't exist (must have been removed)
          json.writeString("(children gone)");
        }

        json.endArray();
      }

      json.endObject();
      return true;
    }

    String time(long ms) {
      return (new Date(ms)).toString() + " (" + ms + ")";
    }

    public void writeKeyValue(JSONWriter json, String k, Object v, boolean isFirst) {
      if (!isFirst) {
        json.writeValueSeparator();
      }
      if (indent) {
        json.indent();
      }
      json.writeString(k);
      json.writeNameSeparator();
      json.write(v);
    }

    boolean printZnode(JSONWriter json, String path) throws IOException {
      try {
        Stat stat = new Stat();
        // Trickily, the call to zkClient.getData fills in the stat variable
        byte[] data = zkClient.getData(path, null, stat, true);

        json.writeString("znode");
        json.writeNameSeparator();
        json.startObject();

        writeKeyValue(json, "path", path, true);

        json.writeValueSeparator();
        json.writeString("prop");
        json.writeNameSeparator();
        json.startObject();
        writeKeyValue(json, "version", stat.getVersion(), true);
        writeKeyValue(json, "aversion", stat.getAversion(), false);
        writeKeyValue(json, "children_count", stat.getNumChildren(), false);
        writeKeyValue(json, "ctime", time(stat.getCtime()), false);
        writeKeyValue(json, "cversion", stat.getCversion(), false);
        writeKeyValue(json, "czxid", stat.getCzxid(), false);
        writeKeyValue(json, "dataLength", stat.getDataLength(), false);
        writeKeyValue(json, "ephemeralOwner", stat.getEphemeralOwner(), false);
        writeKeyValue(json, "mtime", time(stat.getMtime()), false);
        writeKeyValue(json, "mzxid", stat.getMzxid(), false);
        writeKeyValue(json, "pzxid", stat.getPzxid(), false);
        json.endObject();

        if (data != null) {
          writeKeyValue(json, "data", new BytesRef(data).utf8ToString(), false);
        }
        json.endObject();
      } catch (KeeperException e) {
        writeError(500, e.toString());
        return false;
      } catch (InterruptedException e) {
        writeError(500, e.toString());
        return false;
      }
      return true;
    }
  }
}
