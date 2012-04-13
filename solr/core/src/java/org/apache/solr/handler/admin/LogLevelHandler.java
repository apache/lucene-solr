/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.impl.StaticLoggerBinder;


/**
 * A request handler to show which loggers are registered and allows you to set them
 *
 * @since 4.0
 */
public class LogLevelHandler extends RequestHandlerBase {
  public static final String ROOT_NAME = "root";

  //-------------------------------------------------------------------------------------------------
  //
  //   Logger wrapper classes
  //
  //-------------------------------------------------------------------------------------------------

  public abstract static class LoggerWrapper implements Comparable<LoggerWrapper> {
    protected final String name;
    protected String level;

    public LoggerWrapper(String name) {
      this.name = name;
    }

    public String getLevel() {
      return level;
    }

    public String getName() {
      return name;
    }
    
    public abstract boolean isSet();

    public SimpleOrderedMap<?> getInfo() {
      SimpleOrderedMap info = new SimpleOrderedMap();
      info.add("name", getName());
      info.add("level", getLevel());
      info.add("set", isSet());
      return info;
    }

    @Override
    public int compareTo(LoggerWrapper other) {
      if (this.equals(other))
        return 0;

      String tN = this.getName();
      String oN = other.getName();

      if(ROOT_NAME.equals(tN))
        return -1;
      if(ROOT_NAME.equals(oN))
        return 1;

      return tN.compareTo(oN);
    }
  }

  public static interface LoggerFactoryWrapper {
    public String getName();
    public List<String> getAllLevels();
    public void setLogLevel(String category, String level);
    public Collection<LoggerWrapper> getLoggers();
  }


  //-------------------------------------------------------------------------------------------------
  //
  //   java.util.logging
  //
  //-------------------------------------------------------------------------------------------------


  public static class LoggerFactoryWrapperJUL implements LoggerFactoryWrapper {

    @Override
    public String getName() {
      return "java.util.logging";
    }

    @Override
    public List<String> getAllLevels() {
      return Arrays.asList(
        Level.FINEST.getName(),
        Level.FINE.getName(),
        Level.CONFIG.getName(),
        Level.INFO.getName(),
        Level.WARNING.getName(),
        Level.SEVERE.getName(),
        Level.OFF.getName() );
    }

    @Override
    public void setLogLevel(String category, String level) {
      if(ROOT_NAME.equals(category)) {
        category = "";
      }
      
      Logger log = LogManager.getLogManager().getLogger(category);
      if(level==null||"unset".equals(level)||"null".equals(level)) {
        if(log!=null) {
          log.setLevel(null);
        }
      }
      else {
        if(log==null) {
          log = Logger.getLogger(category); // create it
        }
        log.setLevel(Level.parse(level));
      }
    }

    @Override
    public Collection<LoggerWrapper> getLoggers() {
      LogManager manager = LogManager.getLogManager();

      Logger root = manager.getLogger("");
      Map<String,LoggerWrapper> map = new HashMap<String,LoggerWrapper>();
      Enumeration<String> names = manager.getLoggerNames();
      while (names.hasMoreElements()) {
        String name = names.nextElement();
        Logger logger = Logger.getLogger(name);
        if( logger == root) {
          continue;
        }
        map.put(name, new LoggerWrapperJUL(name, logger));

        while (true) {
          int dot = name.lastIndexOf(".");
          if (dot < 0)
            break;
          name = name.substring(0, dot);
          if(!map.containsKey(name)) {
            map.put(name, new LoggerWrapperJUL(name, null));
          }
        }
      }
      map.put(ROOT_NAME, new LoggerWrapperJUL(ROOT_NAME, root));
      return map.values();
    }
  }

  public static class LoggerWrapperJUL extends LoggerWrapper {
    private static final Level[] LEVELS = {
        null, // aka unset
        Level.FINEST,
        Level.FINE,
        Level.CONFIG,
        Level.INFO,
        Level.WARNING,
        Level.SEVERE,
        Level.OFF
        // Level.ALL -- ignore. It is useless.
    };

    final Logger logger;

    public LoggerWrapperJUL(String name, Logger logger) {
      super(name);
      this.logger = logger;
    }

    @Override
    public String getLevel() {
      if(logger==null) {
        return null;
      }
      Level level = logger.getLevel();
      if (level != null) {
        return level.getName();
      }
      for (Level l : LEVELS) {
        if (l == null) {
          // avoid NPE
          continue;
        }
        if (logger.isLoggable(l)) {
          // return first level loggable
          return l.getName();
        }
      }
      return Level.OFF.getName();
    }
    
    @Override
    public boolean isSet() {
      return (logger!=null && logger.getLevel()!=null);
    }
  }

  //-------------------------------------------------------------------------------------------------
  //
  //   Log4j
  //
  //-------------------------------------------------------------------------------------------------

  public static class LoggerWrapperLog4j extends LoggerWrapper {
    final org.apache.log4j.Logger logger;

    public LoggerWrapperLog4j(String name, org.apache.log4j.Logger logger) {
      super(name);
      this.logger = logger;
    }

    @Override
    public String getLevel() {
      if(logger==null) {
        return null;
      }
      Object level = logger.getLevel();
      if(level==null) {
        return null;
      }
      return level.toString();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean isSet() {
      return (logger!=null && logger.getLevel()!=null);
    }
  }

  public static class LoggerFactoryWrapperLog4j implements LoggerFactoryWrapper {

    @Override
    public String getName() {
      return "log4j";
    }

    @Override
    public List<String> getAllLevels() {
      return Arrays.asList(
          org.apache.log4j.Level.ALL.toString(),
          org.apache.log4j.Level.TRACE.toString(),
          org.apache.log4j.Level.DEBUG.toString(),
          org.apache.log4j.Level.INFO.toString(),
          org.apache.log4j.Level.WARN.toString(),
          org.apache.log4j.Level.ERROR.toString(),
          org.apache.log4j.Level.FATAL.toString(),
          org.apache.log4j.Level.OFF.toString());
    }

    @Override
    public void setLogLevel(String category, String level) {
      if(ROOT_NAME.equals(category)) {
        category = "";
      }
      org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(category);
      if(level==null||"unset".equals(level)||"null".equals(level)) {
        setLevelWithReflection(log,null);
      }
      else {
        setLevelWithReflection(log,org.apache.log4j.Level.toLevel(level));
      }
    }
    
    /**
     * log.setLevel(level);
     */
    private void setLevelWithReflection(org.apache.log4j.Logger log, org.apache.log4j.Level level) {
      try {
        Class<?> logclass = Class.forName("org.apache.log4j.Logger");
        Class<?> levelclass = Class.forName("org.apache.log4j.Level");
        Method method = logclass.getMethod("setLevel", levelclass);
        method.invoke(log, level);
      }
      catch(Exception ex) {
        throw new RuntimeException("Unable to set Log4j Level", ex);
      }
    }

    @Override
    public Collection<LoggerWrapper> getLoggers() {

      org.apache.log4j.Logger root = org.apache.log4j.LogManager.getRootLogger();
      Map<String,LoggerWrapper> map = new HashMap<String,LoggerWrapper>();
      Enumeration<?> loggers = org.apache.log4j.LogManager.getCurrentLoggers();
      while (loggers.hasMoreElements()) {
        org.apache.log4j.Logger logger = (org.apache.log4j.Logger)loggers.nextElement();
        String name = logger.getName();
        if( logger == root) {
          continue;
        }
        map.put(name, new LoggerWrapperLog4j(name, logger));

        while (true) {
          int dot = name.lastIndexOf(".");
          if (dot < 0)
            break;
          name = name.substring(0, dot);
          if(!map.containsKey(name)) {
            map.put(name, new LoggerWrapperJUL(name, null));
          }
        }
      }
      map.put(ROOT_NAME, new LoggerWrapperLog4j(ROOT_NAME, root));
      return map.values();
    }
  }
  

  //-------------------------------------------------------------------------------------------------
  //
  //   The Request Handler
  //
  //-------------------------------------------------------------------------------------------------

  LoggerFactoryWrapper factory;
  String slf4jImpl = null;

  @Override
  public void init(NamedList args) {
    String fname = (String)args.get("logger.factory");
    try {
      slf4jImpl = StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr();
      if(fname == null ) {
        if( slf4jImpl.indexOf("Log4j") > 0) {
          fname = "Log4j";
        }
        else if( slf4jImpl.indexOf("JDK") > 0) {
          fname = "JUL";
        }
        else {
          return; // unsuppored
        }
      }
    }
    catch(Exception ex) {}
    
    if("JUL".equalsIgnoreCase(fname)) {
      factory = new LoggerFactoryWrapperJUL();
    }
    else if( "Log4j".equals(fname) ) {
      factory = new LoggerFactoryWrapperLog4j();
    }
    else {
      try {
        factory = (LoggerFactoryWrapper) Class.forName(fname).newInstance();
      }
      catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Don't do anything if the framework is unknown
    if(factory==null) {
      rsp.add("error", "Unsupported Logging Framework: "+slf4jImpl);
      return;
    }
    
    SolrParams params = req.getParams();
    String[] set = params.getParams("set");
    if (set != null) {
      for (String pair : set) {
        String[] split = pair.split(":");
        if (split.length != 2) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Invalid format, expected level:value, got " + pair);
        }
        String category = split[0];
        String level = split[1];

        factory.setLogLevel(category, level);
      }
    }

    rsp.add("framework", factory.getName());
    rsp.add("slfj4", slf4jImpl);
    rsp.add("levels", factory.getAllLevels());

    List<LoggerWrapper> loggers = new ArrayList<LogLevelHandler.LoggerWrapper>(factory.getLoggers());
    Collections.sort(loggers);

    List<SimpleOrderedMap<?>> info = new ArrayList<SimpleOrderedMap<?>>();
    for(LoggerWrapper wrap:loggers) {
      info.add(wrap.getInfo());
    }
    rsp.add("loggers", info);
    rsp.setHttpCaching(false);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Lucene Log Level info";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
