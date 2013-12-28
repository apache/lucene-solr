package org.apache.lucene.server.handlers;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.server.params.FloatType;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.ListType;
import org.apache.lucene.server.params.LongType;
import org.apache.lucene.server.params.OrType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.PolyType;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.server.params.Type;
import org.apache.lucene.server.params.WrapType;

// TODO: use some ... standard markup language/processor!

/** Handles generating live documentation, accessible via
 *  http://localhost:4000/doc", from all registered
 *  handlers. */
public class DocHandler {

  /** Sole constructor. */
  public DocHandler() {
  }

  // nocommit this is wasteful ... use Apache commons?
  private static String escapeHTML(String s) {
    s = s.replaceAll("&", "&amp;");
    s = s.replaceAll(">", "&gt;");
    s = s.replaceAll("<", "&lt;");
    s = s.replaceAll("\"", "&quot;");
    return s;
  }

  private final static Pattern reLink = Pattern.compile("@([a-zA-Z0-9:\\-\\.]+)\\b");

  private final static String LUCENE_DOC_VERSION = "4_2_0";

  private static String expandLinks(String s) {
    StringBuilder sb = new StringBuilder();
    int upto = 0;
    Matcher m = reLink.matcher(s);
    while(true) {
      if (!m.find(upto)) {
        sb.append(s.substring(upto));
        break;
      }
      String group = m.group(1);
      String url;
      String text;
      if (group.startsWith("lucene:")) {
        // External lucene link:
        String[] parts = group.split(":");
        if (parts.length == 3) {
          url = "http://lucene.apache.org/core/" + LUCENE_DOC_VERSION + "/" + parts[1] + "/" + parts[2].replace('.', '/') + ".html";
          text = "Lucene Javadocs";
        } else {
          throw new IllegalArgumentException("malformed lucene link: " + group);
        }
      } else {
        // Internal link:
        text = group;
        url = "/doc?method=" + group;
      }
      sb.append(s.substring(upto, m.start(1)-1));
      sb.append("<a href=\"");
      sb.append(url);
      sb.append("\">");
      sb.append(text);
      sb.append("</a>");
      upto = m.end(1);
    }
    return sb.toString();
  }

  /** Generates documentation for the provided handlers.  If
   *  params is non-empty, generates documentation for a
   *  specific method. */
  public String handle(Map<String,List<String>> params, Map<String,Handler> handlers) {
    StringBuilder sb = new StringBuilder();
    sb.append("<html>");
    sb.append("<head>");
    sb.append("<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">");
    sb.append("</head>");
    sb.append("<body>");
    if (params.size() == 0) {
      sb.append("<h1>Lucene Server</h1>");
      sb.append("Available methods:\n");
      sb.append("<dd>");
      List<String> keys = new ArrayList<String>(handlers.keySet());
      Collections.sort(keys);
      for(String key : keys) {
        sb.append("<dt><a href=\"/doc?method=");
        sb.append(key);
        sb.append("\">");
        sb.append(escapeHTML(key));
        sb.append("</a>");
        sb.append("</dt>");
        sb.append("<dd>");
        Handler handler = handlers.get(key);
        sb.append(expandLinks(handler.getTopDoc()));
        sb.append("</dd>");
      }
      sb.append("</dd>");
    } else {
      String method = params.get("method").get(0);
      Handler handler = handlers.get(method);
      if (handler == null) {
        throw new IllegalArgumentException("unknown method \"" + method + "\"");
      }
      sb.append("<h1>Lucene Server: ");
      sb.append(method);
      sb.append("</h1>");
      sb.append(expandLinks(handler.getTopDoc()));
      sb.append("<br>");
      sb.append("<br><b>Parameters</b>:<br>");
      StructType type = handler.getType();
      Set<StructType> seen = Collections.newSetFromMap(new IdentityHashMap<StructType,Boolean>());
      renderStructType(seen, sb, type);
    }
    sb.append("</body>");
    sb.append("</html>");
    return sb.toString();
  }

  private String simpleTypeToString(Type type) {
    if (type instanceof IntType) {
      return "int";
    } else if (type instanceof FloatType) {
      return "float";
    } else if (type instanceof LongType) {
      return "long";
    } else if (type instanceof StringType) {
      return "string";
    } else if (type instanceof OrType) {
      String s1 = null;
      for(Type subType : ((OrType) type).types) {
        String sub = simpleTypeToString(subType);
        if (sub != null) {
          if (s1 == null) {
            s1 = sub;
          } else {
            s1 += " or " + sub;
          }
        } else {
          return null;
        }
      }
      return s1;
    } else if (type instanceof ListType) {
      String s1 = simpleTypeToString(((ListType) type).subType);
      if (s1 != null) {
        return "List of " + s1;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  private void renderStructType(Set<StructType> seen, StringBuilder sb, StructType type) {
    if (seen.contains(type)) {
      sb.append("<br><br>See <a href=\"\">here</a>.");
      return;
    }
    seen.add(type);
    sb.append("<dl>");
    List<String> args = new ArrayList<String>(type.params.keySet());
    Collections.sort(args);
    for(String arg : args) {
      Param p = type.params.get(arg);
      sb.append("<br>");
      sb.append("<dt><code><b>");
      sb.append(escapeHTML(arg));
      sb.append("</b>");
      String s = simpleTypeToString(p.type);
      if (p.defaultValue != null) {
        if (s != null) {
          s += "; default: ";
        } else {
          s = "default: ";
        }
        s += p.defaultValue;
      }
      if (s != null) {
        sb.append(" (");
        sb.append(s);
        sb.append(")");
      }
      sb.append("</code></dt>");
      sb.append("<dd>");
      sb.append(expandLinks(p.desc));

      Type subType = p.type;
      if (subType instanceof WrapType) {
        subType = ((WrapType) subType).getWrappedType();
      }

      if (subType instanceof ListType) {
        ListType lt = (ListType) subType;
        sb.append("<br><br>");
        if (lt.subType instanceof StructType) {
          sb.append("List of:");
          renderStructType(seen, sb, (StructType) lt.subType);
        }
      } else if (subType instanceof StructType) {
        renderStructType(seen, sb, (StructType) subType);
      } else if (subType instanceof PolyType) {
        PolyType pt = (PolyType) subType;
        List<String> polyKeys = new ArrayList<String>(pt.types.keySet());
        Collections.sort(polyKeys);
        sb.append("<dl>");
        for(String key : polyKeys) {
          sb.append("<dt>= ");
          sb.append("<code>");
          sb.append(escapeHTML(key));
          sb.append("</code>");
          sb.append("</dt>");
          sb.append("<dd>");
          sb.append(expandLinks(pt.types.get(key).desc));
          renderStructType(seen, sb, pt.types.get(key).type);
          sb.append("</dd>");
        }
        sb.append("</dl>");
      }
      sb.append("</dd>");
    }
    sb.append("</dl>");
  }
}

