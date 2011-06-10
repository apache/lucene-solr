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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;


/**
 * Admin JDK Logger level report and selection servlet.
 *
 *
 * @since solr 1.3
 */
public final class LogLevelSelection extends HttpServlet {
  @Override
  public void init() throws ServletException {
  }

  /**
   * Processes an HTTP GET request and changes the logging level as
   * specified.
   */
  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
          throws IOException, ServletException {
    // Output page

    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.write("<html><head>\n");
    out.write("<title>Solr Admin: JDK Log Level Selector</title>\n");
    out.write("<link rel=\"stylesheet\" type=\"text/css\" href=\"solr-admin.css\" />");
    out.write("</head><body>\n");
    out.write("<a href=\".\"><img border=\"0\" align=\"right\" height=\"78\" width=\"142\" src=\"solr_small.png\" alt=\"Solr\"></a>");
    out.write("<h1>JDK Log Level Selector</h1>");

    out.write("<p>Below is the complete JDK Log hierarchy with " +
            "intermediate logger/categories synthesized.  " +
            "The effective logging level is shown to the " +
            "far right. If a logger has unset level, then " +
            "the effective level is that of the nearest ancestor " +
            "with a level setting.  Note that this only shows " +
            "JDK Log levels.</p>\n");

    out.write("<form method='POST'>\n");

    out.write("<input type='submit' name='submit' value='set' " +
            "class='button'>\n");
    out.write("<input type='submit' name='submit' value='cancel' " +
            "class='button'>\n");
    out.write("<br><br>\n");

    out.write("<table cellspacing='2' cellpadding='2'>");

    out.write("<tr bgcolor='#CCCCFF'>" +
            "<th align=left>Logger/Category name<br>" +
            "<th colspan=9>Level</th>" +
            "</tr><tr bgcolor='#CCCCFF'>" +
            "<td bgcolor='#AAAAAA'>" +
            "(Dark rows don't yet exist.)</td>");

    for (int j = 0; j < LEVELS.length; ++j) {
      out.write("<th align=left>");
      if (LEVELS[j] != null) out.write(LEVELS[j].toString());
      else out.write("unset");
      out.write("</th>");
    }
    out.write("<th align=left>Effective</th>\n");
    out.write("</tr>\n");

    Iterator iWrappers = buildWrappers().iterator();
    while (iWrappers.hasNext()) {

      LogWrapper wrapper = (LogWrapper) iWrappers.next();

      out.write("<tr");
      if (wrapper.logger == null) {
        out.write(" bgcolor='#AAAAAA'");
      }
      //out.write( ( wrapper.logger != null ) ? "#DDDDDD" : "#AAAAAA" );
      out.write("><td>");
      if ("".equals(wrapper.name)) {
        out.write("root");
      } else {
        out.write(wrapper.name);
      }
      out.write("</td>\n");
      for (int j = 0; j < LEVELS.length; ++j) {
        out.write("<td align=center>");
        if (!wrapper.name.equals("root") ||
                (LEVELS[j] != null)) {
          out.write("<input type='radio' name='");
          if ("".equals(wrapper.name)) {
            out.write("root");
          } else {
            out.write(wrapper.name);
          }
          out.write("' value='");
          if (LEVELS[j] != null) out.write(LEVELS[j].toString());
          else out.write("unset");
          out.write('\'');
          if (LEVELS[j] == wrapper.level()) out.write(" checked");
          out.write('>');
        }
        out.write("</td>\n");
      }
      out.write("<td align=center>");
      if (wrapper.logger != null) {
        out.write(getEffectiveLevel(wrapper.logger).toString());
      }
      out.write("</td></tr>\n");
    }
    out.write("</table>\n");

    out.write("<br>\n");
    out.write("<input type='submit' name='submit' value='set' " +
            "class='button'>\n");
    out.write("<input type='submit' name='submit' value='cancel' " +
            "class='button'>\n");

    out.write("</form>\n");

    out.write("</body></html>\n");
  }


  @Override
  public void doPost(HttpServletRequest request,
                     HttpServletResponse response)
          throws IOException, ServletException {
    if (request.getParameter("submit").equals("set")) {

      Map paramMap = request.getParameterMap();

      Iterator iParams = paramMap.entrySet().iterator();
      while (iParams.hasNext()) {
        Map.Entry p = (Map.Entry) iParams.next();
        String name = (String) p.getKey();
        String value = ((String[]) p.getValue())[0];

        if (name.equals("submit")) continue;
        Logger logger;
        LogManager logManager = LogManager.getLogManager();
        if ("root".equals(name)) {
          logger = logManager.getLogger("");
        } else logger = logManager.getLogger(name);

        if ("unset".equals(value)) {
          if ((logger != null) && (logger.getLevel() != null)) {
            logger.setLevel(null);
            log.info("Unset log level on '" + name + "'.");
          }
        } else {
          Level level = Level.parse(value);
          if (logger == null) logger = Logger.getLogger(name);
          if (logger.getLevel() != level) {
            logger.setLevel(level);
            log.info("Set '" + name + "' to " +
                    level + " level.");
          }
        }
      }
    } else {
      log.fine("Selection form cancelled");
    }

    // Redirect back to standard get page.
    response.sendRedirect(request.getRequestURI());
  }


  private Collection buildWrappers() {
    // Use tree to get sorted results
    SortedSet<LogWrapper> roots = new TreeSet<LogWrapper>();

    roots.add(LogWrapper.ROOT);

    LogManager logManager = LogManager.getLogManager();

    Enumeration<String> loggerNames = logManager.getLoggerNames();
    while (loggerNames.hasMoreElements()) {
      String name = loggerNames.nextElement();
      Logger logger = Logger.getLogger(name);
      LogWrapper wrapper = new LogWrapper(logger);
      roots.remove(wrapper); // Make sure add occurs
      roots.add(wrapper);

      while (true) {
        int dot = name.lastIndexOf(".");
        if (dot < 0) break;
        name = name.substring(0, dot);
        roots.add(new LogWrapper(name)); // if not already
      }
    }

    return roots;
  }

  private Level getEffectiveLevel(Logger logger) {
    Level level = logger.getLevel();
    if (level != null) {
      return level;
    }
    for (Level l : LEVELS) {
      if (l == null) {
        // avoid NPE
        continue;
      }
      if (logger.isLoggable(l)) {
        // return first level loggable
        return l;
      }
    }
    return Level.OFF;
  }

  private static class LogWrapper
          implements Comparable {
    public static LogWrapper ROOT =
            new LogWrapper(LogManager.getLogManager().getLogger(""));

    public LogWrapper(Logger logger) {
      this.logger = logger;
      this.name = logger.getName();
    }

    public LogWrapper(String name) {
      this.name = name;
    }


    public int compareTo(Object other) {
      if (this.equals(other)) return 0;
      if (this == ROOT) return -1;
      if (other == ROOT) return 1;

      return name.compareTo(((LogWrapper) other).name);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      LogWrapper other = (LogWrapper) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      return true;
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
    }

    public Level level() {
      if (logger != null) return logger.getLevel();
      return null;
    }

    public Logger logger = null;
    public String name;
  }

  private static Level[] LEVELS = {
          null, // aka unset
          Level.FINEST,
          Level.FINE,
          Level.CONFIG,
          Level.INFO,
          Level.WARNING,
          Level.SEVERE,
          Level.OFF
          // Level.ALL -- ignore.  It is useless.
  };

  private Logger log = Logger.getLogger(getClass().getName());
}
