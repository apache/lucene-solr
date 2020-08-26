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

package org.apache.solr.response;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.solr.client.solrj.io.graph.Traversal;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.GraphHandler;
import org.apache.solr.request.SolrQueryRequest;


public class GraphMLResponseWriter implements QueryResponseWriter {

  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    /* NOOP */
  }

  public String getContentType(SolrQueryRequest req, SolrQueryResponse res) {
    return "application/xml";
  }

  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse res) throws IOException {

    Exception e1 = res.getException();
    if(e1 != null) {
      e1.printStackTrace(new PrintWriter(writer));
      return;
    }

    TupleStream stream =  (TupleStream)req.getContext().get("stream");

    if(stream instanceof GraphHandler.DummyErrorStream) {
      GraphHandler.DummyErrorStream d = (GraphHandler.DummyErrorStream)stream;
      Exception e = d.getException();
      e.printStackTrace(new PrintWriter(writer));
      return;
    }


    Traversal traversal = (Traversal)req.getContext().get("traversal");
    PrintWriter printWriter = new PrintWriter(writer);

    try {

      stream.open();

      Tuple tuple = null;

      int edgeCount = 0;

      printWriter.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      printWriter.println("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" ");
      printWriter.println("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ");
      printWriter.print("xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns ");
      printWriter.println("http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">");

      printWriter.println("<graph id=\"G\" edgedefault=\"directed\">");

      while (true) {
        //Output the graph
        tuple = stream.read();
        if (tuple.EOF) {
          break;
        }

        String id = tuple.getString("node");

        if (traversal.isMultiCollection()) {
          id = tuple.getString("collection") + "." + id;
        }

        printWriter.write("<node id=\""+ xmlEscape(id)+"\"");

        List<String> outfields = new ArrayList<>();
        Iterator<Object> keys = tuple.getFields().keySet().iterator();
        while(keys.hasNext()) {
          String key = String.valueOf(keys.next());
          if(key.equals("node") || key.equals("ancestors") || key.equals("collection")) {
            continue;
          } else {
            outfields.add(key);
          }
        }

        if (outfields.size() > 0) {
          printWriter.println(">");
          for (String nodeAttribute : outfields) {
            Object o = tuple.get(nodeAttribute);
            if (o != null) {
              printWriter.println("<data key=\"" + xmlEscape(nodeAttribute) + "\">" + xmlEscape(o.toString()) + "</data>");
            }
          }
          printWriter.println("</node>");
        } else {
          printWriter.println("/>");
        }

        List<String> ancestors = tuple.getStrings("ancestors");

        if(ancestors != null) {
          for (String ancestor : ancestors) {
            ++edgeCount;
            printWriter.write("<edge id=\"" + edgeCount + "\" ");
            printWriter.write(" source=\"" + xmlEscape(ancestor) + "\" ");
            printWriter.println(" target=\"" + xmlEscape(id) + "\"/>");
          }
        }
      }

      printWriter.write("</graph></graphml>");
    } finally {
      stream.close();
    }
  }

  private String xmlEscape(String s) {
    if(s.indexOf(">") > -1) {
      s = s.replace(">", "&gt;");
    }

    if(s.indexOf("<") > -1) {
      s = s.replace("<", "&lt;");
    }

    if(s.indexOf("\"")> -1) {
      s = s.replace("\"", "&quot;");
    }

    if(s.indexOf("'") > -1) {
      s = s.replace("'", "&apos;");
    }

    if(s.indexOf("&") > -1) {
      s = s.replace("&", "&amp;");
    }

    return s;
  }
}
