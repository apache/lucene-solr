/**
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
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


/**
 * Zookeeper Info
 *
 * @since solr 4.0
 */
public final class ZookeeperInfoServlet extends HttpServlet {

  @Override
  public void init() throws ServletException {
  }

  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
          throws IOException, ServletException {
    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");

    CoreContainer cores = (CoreContainer) request.getAttribute("org.apache.solr.CoreContainer");
    
    String path = request.getParameter("path");
    String addr = request.getParameter("addr");
    
    if (addr != null && addr.length() == 0)
    {
      addr = null;
    }
    
    String detailS = request.getParameter("detail");
    boolean detail = detailS != null && detailS.equals("true");
    PrintWriter out = response.getWriter();
    
    ZKPrinter printer = new ZKPrinter(response, out, cores.getZkController(), addr);
    printer.detail = detail;
    
    try {
      printer.print(path);
    } finally {
      printer.close();
    }
  }

  @Override
  public void doPost(HttpServletRequest request,
                     HttpServletResponse response)
          throws IOException, ServletException {
    doGet(request,response);
  }


  //--------------------------------------------------------------------------------------
  // 
  //--------------------------------------------------------------------------------------
  
  static class ZKPrinter
  {
  
    static boolean FULLPATH_DEFAULT = false;
  
    boolean indent = true;
    boolean fullpath = FULLPATH_DEFAULT;
    boolean detail = false;
  
    String addr; // the address passed to us
    String keeperAddr; // the address we're connected to
  
    boolean doClose;  // close the client after done if we opened it
  
    HttpServletResponse response;
    PrintWriter out;
    SolrZkClient zkClient;
  
    int level;
    int maxData = 95;
  
    public ZKPrinter(HttpServletResponse response, PrintWriter out, ZkController controller, String addr) throws IOException
    {
      this.response = response;
      this.out = out;
      this.addr = addr;
      
      if (addr == null)
      {
        if (controller != null)
        {
          // this core is zk enabled
          keeperAddr = controller.getZkServerAddress();
          zkClient = controller.getZkClient();
          if (zkClient != null && zkClient.isConnected())
          {
            return;
          }
          else
          {
            // try a different client with this address
            addr = keeperAddr;
          }
        }
      }
  
      keeperAddr = addr;
      if (addr == null)
      {
        response.setStatus(404);
        out.println
        (
          "{" +
          "\"status\": 404" +
          ", \"error\" : \"Zookeeper is not configured for this Solr Core. Please try connecting to an alternate zookeeper address.\"" +
          "}"
        );
        return;
      }
  
      try
      {
        zkClient = new SolrZkClient(addr, 10000);
        doClose = true;
      }
      catch (TimeoutException e)
      {
        response.setStatus(503);
        out.println
        (
          "{" +
          "\"status\": 503" +
          ", \"error\" : \"Could not connect to zookeeper at '" + addr + "'\"" +
          "}"
        );
        zkClient = null;
        return;
      }
      catch (InterruptedException e)
      {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        response.setStatus(503);
        out.println
        (
          "{" +
          "\"status\": 503" +
          ", \"error\" : \"Could not connect to zookeeper at '" + addr + "'\"" +
          "}"
        );
        zkClient = null;
        return;
      }
  
    }

    public void close()
    {
      try {
        if (doClose)
        {
          zkClient.close();
        }
      } catch (InterruptedException e) {
          // ignore exception on close
      }
    }
  
    // main entry point
    void print(String path) throws IOException
    {
      if (zkClient == null) {
        return;
      }
  
      // normalize path
      if (path == null) {
        path = "/";
      }
      else {
        path.trim();
        if (path.length() == 0)
        {
          path = "/";
        }
      }
      
      if (path.endsWith("/") && path.length() > 1)
      {
        path = path.substring(0, path.length() - 1);
      }
  
      int idx = path.lastIndexOf('/');
      String parent = idx >= 0 ? path.substring(0, idx) : path;
      if (parent.length() == 0)
      {
        parent = "/";
      }
  
      out.println("{");
  
      if (detail)
      {
        printZnode(path);
        out.println(", ");
      }
  
      out.println("\"tree\" : [");
      printTree(path);
      out.println("]");
  
      out.println("}");
    }
  
    void exception(Exception e)
    {
      response.setStatus(500);
      out.println
      (
        "{" +
        "\"status\": 500" +
        ", \"error\" : \"" + e.toString() + "\"" +
        "}"
      );
    }
  
    void xmlescape(String s)
    {
      try
      {
        XML.escapeCharData(s, out);
      }
      catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
  
    // collapse all whitespace to a single space or escaped newline
    String compress(String str) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < str.length(); i++) {
        char ch = str.charAt(i);
        boolean whitespace = false;
        boolean newline = false;
        while (Character.isWhitespace(ch)) {
          whitespace = true;
          if (ch == '\n')
            newline = true;
          if (++i >= str.length())
            return sb.toString();
          ch = str.charAt(i);
        }

        if (newline) {
          // sb.append("\\n");
          sb.append("  ");  // collapse newline to two spaces
        } else if (whitespace) {
          sb.append(' ');
        }

        // TODO: handle non-printable chars
        sb.append(ch);

        if (sb.length() >= maxData)
          return sb.toString() + " ...";
      }
      return sb.toString();
    }

    void url(String label, String path, boolean detail) throws IOException {
      try {
        out.print("<a href=\"zookeeper?");
        if (path != null) {
          out.print("path=");
          out.print(URLEncoder.encode(path, "UTF-8"));
        }
        if (detail) {
          out.print("&detail=" + detail);
        }
        if (fullpath != FULLPATH_DEFAULT) {
          out.print("&fullpath=" + fullpath);
        }
        if (addr != null) {
          out.print("&addr=");
          out.print(URLEncoder.encode(addr, "UTF-8"));
        }

        out.print("\">");
        xmlescape(label);
        out.print("</a>");

      } catch (UnsupportedEncodingException e) {
        exception(e);
      }
    }

    void printTree(String path) throws IOException
    {
      String label = path;
      if (!fullpath)
      {
        int idx = path.lastIndexOf('/');
        label = idx > 0 ? path.substring(idx + 1) : path;
      }
  
      //url(label, path, true);
      out.println("{");
      out.println("\"data\" : \"" + label + "\"");
  
      Stat stat = new Stat();
      try
      {
        byte[] data = zkClient.getData(path, null, stat, true);
  
        if( stat.getEphemeralOwner() != 0 )
        {
          out.println(", \"ephemeral\" : true");
          out.println(", \"version\" : \"" + stat.getVersion() + "\"");
        }
        
        /*
        if (stat.getNumChildren() != 0)
        {
          out.println(", \"children_count\" : \"" + stat.getNumChildren() + "\"");
        }
        */
  
        //if (data != null)
        if( stat.getDataLength() != 0 )
        {
          String str;
          try
          {
            str = new String(data, "UTF-8");
            str = str.replaceAll("\\\"", "\\\\\"");
  
            out.print(", \"content\" : \"");
            //xmlescape(compress(str));
            out.print(compress(str));
            out.println("\"");
          }
          catch (UnsupportedEncodingException e)
          {
            // not UTF8
            StringBuilder sb = new StringBuilder("BIN(");
            sb.append("len=" + data.length);
            sb.append("hex=");
            int limit = Math.min(data.length, maxData / 2);
            for (int i = 0; i < limit; i++)
            {
              byte b = data[i];
              sb.append(StrUtils.HEX_DIGITS[(b >> 4) & 0xf]);
              sb.append(StrUtils.HEX_DIGITS[b & 0xf]);
            }
            if (limit != data.length)
            {
              sb.append("...");
            }
            sb.append(")");
            str = sb.toString();
            //out.print(str);
          }
        }
      }
      catch (IllegalArgumentException e)
      {
        // path doesn't exist (must have been removed)
        out.println("(path gone)");
      }
      catch (KeeperException e)
      {
        e.printStackTrace();
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
  
      if( stat.getNumChildren() > 0 )
      {
        out.print(", \"children\" : [");
  
        List<String> children = null;
        try
        {
          children = zkClient.getChildren(path, null, true);
        }
        catch (KeeperException e)
        {
          exception(e);
          return;
        }
        catch (InterruptedException e)
        {
          exception(e);
        }
        catch (IllegalArgumentException e)
        {
          // path doesn't exist (must have been removed)
          out.println("(children gone)");
        }
  
        Integer i = 0;
        for( String child : children )
        {
          if( 0 != i )
          {
            out.print(", ");
          }
  
          String childPath = path + (path.endsWith("/") ? "" : "/") + child;
          printTree( childPath );
  
          i++;
        }
  
        out.println("]");
      }
  
      out.println("}");
    }

    String time(long ms) {
      return (new Date(ms)).toString() + " (" + ms + ")";
    }
  
    void printZnode(String path) throws IOException
    {
      try
      {
        Stat stat = new Stat();
        byte[] data = zkClient.getData(path, null, stat, true);
  
        out.println("\"znode\" : {");
  
        out.print("\"path\" : \"");
        xmlescape(path);
        out.println("\"");
  
        out.println(", \"version\" : \"" + stat.getVersion() + "\"");
        out.println(", \"aversion\" : \"" + stat.getAversion() + "\"");
        out.println(", \"cversion\" : \"" + stat.getCversion() + "\"");
        out.println(", \"ctime\" : \"" + time(stat.getCtime()) + "\"");
        out.println(", \"mtime\" : \"" + time(stat.getMtime()) + "\"");
        out.println(", \"czxid\" : \"" + stat.getCzxid() + "\"");
        out.println(", \"mzxid\" : \"" + stat.getMzxid() + "\"");
        out.println(", \"pzxid\" : \"" + stat.getPzxid() + "\"");
        out.println(", \"children_count\" : \"" + stat.getNumChildren() + "\"");
        out.println(", \"ephemeralOwner\" : \"" + stat.getEphemeralOwner() + "\"");
        out.println(", \"dataLength\" : \"" + stat.getDataLength() + "\"");
  
        if( stat.getDataLength() != 0 )
        {
          boolean isBinary = false;
          String str;
          try
          {
            str = new String(data, "UTF-8");
          }
          catch (UnsupportedEncodingException e)
          {
            // The results are unspecified
            // when the bytes are not properly encoded.
  
            // not UTF8
            StringBuilder sb = new StringBuilder(data.length * 2);
            for (int i = 0; i < data.length; i++)
            {
              byte b = data[i];
              sb.append(StrUtils.HEX_DIGITS[(b >> 4) & 0xf]);
              sb.append(StrUtils.HEX_DIGITS[b & 0xf]);
              if ((i & 0x3f) == 0x3f)
              {
                sb.append("\n");
              }
            }
            str = sb.toString();
          }
          str = str.replaceAll("\\\"", "\\\\\"");
  
          out.print(", \"data\" : \"");
          //xmlescape(str);
          out.print(str);
          out.println("\"");
        }
  
        out.println("}");
  
      }
      catch (KeeperException e)
      {
        exception(e);
        return;
      }
      catch (InterruptedException e)
      {
        exception(e);
      }
    }
  }
}

