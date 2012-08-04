package org.apache.solr;

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

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.*;

public class SolrLogFormatter extends Formatter {

  /** Add this interface to a thread group and the string returned by
   * getTag() will appear in log statements of any threads under that group.
   */
  public static interface TG {
    public String getTag();
  }

  long startTime = System.currentTimeMillis();
  long lastTime = startTime;
  Map<Method, String> methodAlias = new HashMap<Method, String>();
  
  public static class Method {
    public String className;
    public String methodName;

    public Method(String className, String methodName) {
      this.className = className;
      this.methodName = methodName;
    }
    
    @Override
    public int hashCode() {
      return className.hashCode() + methodName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof  Method)) return false;
      Method other = (Method)obj;
      return (className.equals(other.className) && methodName.equals(other.methodName));
    }

    @Override
    public String toString() {
      return className + '.' + methodName;
    }
  }


  public SolrLogFormatter() {
    super();
    
    methodAlias.put(new Method("org.apache.solr.update.processor.LogUpdateProcessor","finish"), "UPDATE");
    methodAlias.put(new Method("org.apache.solr.core.SolrCore","execute"), "REQ");
  }


  // TODO: name this better... it's only for cloud tests where every core container has just one solr server so Port/Core are fine
  public boolean shorterFormat = false;

  /**  Removes info that is redundant for current cloud tests including core name, webapp, and common labels path= and params=
   * [] webapp=/solr path=/select params={q=foobarbaz} hits=0 status=0 QTime=1
   * /select {q=foobarbaz} hits=0 status=0 QTime=1
   * NOTE: this is a work in progress and different settings may be ideal for other types of tests.
   */
  public void setShorterFormat() {
    shorterFormat = true;
    // looking at /update is enough... we don't need "UPDATE /update"
    methodAlias.put(new Method("org.apache.solr.update.processor.LogUpdateProcessor","finish"), "");
  }

  public static class CoreInfo {
    static int maxCoreNum;
    String shortId;
    String url;
    Map<String, String> coreProps;
  }

  Map<SolrCore, CoreInfo> coreInfoMap = new WeakHashMap<SolrCore, CoreInfo>();    // TODO: use something that survives across a core reload?

  public Map<String,String> classAliases = new HashMap<String, String>();

  @Override
  public String format(LogRecord record) {
    try {
      return _format(record);
    } catch (Throwable th) {
      // logging swallows exceptions, so if we hit an exception we need to convert it to a string to see it
      return "ERROR IN SolrLogFormatter! original message:" + record.getMessage() + "\n\tException: " + SolrException.toStr(th);
    }
  }

  
  public void appendThread(StringBuilder sb, LogRecord record) {
    Thread th = Thread.currentThread();


/******
    sb.append(" T=");
    sb.append(th.getName()).append(' ');

    // NOTE: tried creating a thread group around jetty but we seem to lose it and request
    // threads are in the normal "main" thread group
    ThreadGroup tg = th.getThreadGroup();
    while (tg != null) {
sb.append("(group_name=").append(tg.getName()).append(")");

      if (tg instanceof TG) {
        sb.append(((TG)tg).getTag());
        sb.append('/');
      }
      try {
        tg = tg.getParent();
      } catch (Throwable e) {
        tg = null;
      }
    }
 ******/

    // NOTE: LogRecord.getThreadID is *not* equal to Thread.getId()
    sb.append(" T");
    sb.append(th.getId());
  }

  
  public String _format(LogRecord record) {
    String message = record.getMessage();
    
    StringBuilder sb = new StringBuilder(message.length() + 80);
    
    long now = record.getMillis();
    long timeFromStart = now - startTime;
    long timeSinceLast = now - lastTime;
    lastTime = now;
    String shortClassName = getShortClassName(record.getSourceClassName(), record.getSourceMethodName());

/***
    sb.append(timeFromStart).append(' ').append(timeSinceLast);
    sb.append(' ');
    sb.append(record.getSourceClassName()).append('.').append(record.getSourceMethodName());
    sb.append(' ');
    sb.append(record.getLevel());
***/

    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    SolrQueryRequest req = requestInfo == null ? null : requestInfo.getReq();
    SolrCore core = req == null ? null : req.getCore();
    ZkController zkController = null;
    CoreInfo info = null;
    
    if (core != null) {
      info = coreInfoMap.get(core);
      if (info == null) {
        info = new CoreInfo();
        info.shortId = "C"+Integer.toString(CoreInfo.maxCoreNum++);
        coreInfoMap.put(core, info);

        if (sb.length() == 0) sb.append("ASYNC ");
        sb.append(" NEW_CORE "+info.shortId);
        sb.append(" name=" + core.getName());
        sb.append(" " + core);
      }

      if (zkController == null) {
        zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
      }
      if (zkController != null) {
        if (info.url == null) {
          info.url = zkController.getBaseUrl() + "/" + core.getName();
          sb.append(" url="+info.url + " node="+zkController.getNodeName());
        }

        if(info.coreProps == null) {
          info.coreProps = getCoreProps(zkController, core);
        }

        Map<String, String> coreProps = getCoreProps(zkController, core);
        if(!coreProps.equals(info.coreProps)) {
          info.coreProps = coreProps;
          final String corePropsString = "coll:" + core.getCoreDescriptor().getCloudDescriptor().getCollectionName() + " core:" + core.getName() + " props:" + coreProps;
          sb.append(" " + info.shortId + "_STATE=" + corePropsString);
        }
      }
    }


    if (sb.length() > 0) sb.append('\n');
    sb.append(timeFromStart);

//     sb.append("\nL").append(record.getSequenceNumber());     // log number is useful for sequencing when looking at multiple parts of a log file, but ms since start should be fine.
   appendThread(sb, record);


    if (info != null) {
      sb.append(' ').append(info.shortId);                     // core
    }
    if (zkController != null) {
      sb.append(" P").append(zkController.getHostPort());      // todo: should be able to get this from core container for non zk tests
    }

    if (shortClassName.length() > 0) {
      sb.append(' ').append(shortClassName);
    }

    if (record.getLevel() != Level.INFO) {
      sb.append(' ').append(record.getLevel());
    }

    sb.append(' ');
    appendMultiLineString(sb, message);
    Throwable th = record.getThrown();
    if (th != null) {
      sb.append(' ');
      String err = SolrException.toStr(th);
      String ignoredMsg = SolrException.doIgnore(th, err);
      if (ignoredMsg != null) {
        sb.append(ignoredMsg);
      } else {
        sb.append(err);
      }
    }

    sb.append('\n');

    /*** Isn't core specific... prob better logged from zkController
    if (info != null) {
      ClusterState clusterState = zkController.getClusterState();
      if (info.clusterState != clusterState) {
        // something has changed in the matrix...
        sb.append(zkController.getBaseUrl() + " sees new ClusterState:");
      }
    }
    ***/
    
    return sb.toString();
  }

  private Map<String,String> getCoreProps(ZkController zkController, SolrCore core) {
    final String collection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    ZkNodeProps props = zkController.getClusterState().getShardProps(collection,  ZkStateReader.getCoreNodeName(zkController.getNodeName(), core.getName()));
    if(props!=null) {
      return props.getProperties(); 
    }
    return Collections.EMPTY_MAP;
  }


  private Method classAndMethod = new Method(null,null); // don't need to be thread safe
  private String getShortClassName(String name, String method) {
    classAndMethod.className = name;
    classAndMethod.methodName = method;

    String out = methodAlias.get(classAndMethod);
    if (out != null) return out;

    StringBuilder sb = new StringBuilder();

    int lastDot = name.lastIndexOf('.');
    if (lastDot < 0) return name + '.' + method;

    int prevIndex = -1;
    for (;;) {
      char ch = name.charAt(prevIndex + 1);
      sb.append(ch);
      int idx = name.indexOf('.', prevIndex+1);
      ch = name.charAt(idx+1);
      if (idx >= lastDot || Character.isUpperCase(ch)) {
        sb.append(name.substring(idx));
        break;
      }
      prevIndex = idx;
    }
  
    return sb.toString() + '.' + method;
  }
  
  private void addFirstLine(StringBuilder sb, String msg) {
//    INFO: [] webapp=/solr path=/select params={q=foobarbaz} hits=0 status=0 QTime=1

    if (!shorterFormat || !msg.startsWith("[")) {
      sb.append(msg);      
      return;
    }

    int idx = msg.indexOf(']');
    if (idx < 0 || !msg.startsWith(" webapp=", idx+1)) {
      sb.append(msg);
      return;
    }
    
    idx = msg.indexOf(' ',idx+8); // space after webapp=
    if (idx < 0) { sb.append(msg); return; }
    idx = msg.indexOf('=',idx+1);   // = in  path=
    if (idx < 0) { sb.append(msg); return; }

    int idx2 = msg.indexOf(' ',idx+1);
    if (idx2 < 0) { sb.append(msg); return; }


    sb.append(msg.substring(idx+1, idx2+1));  // path
    
    idx = msg.indexOf("params=", idx2);
    if (idx < 0) {
      sb.append(msg.substring(idx2));
    } else {
      sb.append(msg.substring(idx+7));
    }
  }
  
  private void appendMultiLineString(StringBuilder sb, String msg) {
    int idx = msg.indexOf('\n');
    if (idx < 0) {
      addFirstLine(sb, msg);
      return;
    }

    int lastIdx = -1;
    for (;;) {
      if (idx < 0) {
        if (lastIdx == -1) {
          addFirstLine(sb, msg.substring(lastIdx+1));
        } else {
          sb.append(msg.substring(lastIdx+1));
        }
        break;
      }
      if (lastIdx == -1) {
        addFirstLine(sb, msg.substring(lastIdx+1, idx));
      } else {
        sb.append(msg.substring(lastIdx+1, idx));
      }

      sb.append("\n\t");
      lastIdx = idx;
      idx = msg.indexOf('\n',lastIdx+1);
    }
  }

  @Override
  public String getHead(Handler h) {
    return super.getHead(h);
  }

  @Override
  public String getTail(Handler h) {
    return super.getTail(h);
  }

  @Override
  public String formatMessage(LogRecord record) {
    return format(record);
  }



  static ThreadLocal<String> threadLocal = new ThreadLocal<String>();
  
  public static void main(String[] args) throws Exception {

      Handler[] handlers = Logger.getLogger("").getHandlers();
      boolean foundConsoleHandler = false;
      for (int index = 0; index < handlers.length; index++) {
        // set console handler to SEVERE
        if (handlers[index] instanceof ConsoleHandler) {
          handlers[index].setLevel(Level.ALL);
          handlers[index].setFormatter(new SolrLogFormatter());
          foundConsoleHandler = true;
        }
      }
      if (!foundConsoleHandler) {
        // no console handler found
        System.err.println("No consoleHandler found, adding one.");
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.ALL);
        consoleHandler.setFormatter(new SolrLogFormatter());
        Logger.getLogger("").addHandler(consoleHandler);
      }



    final org.slf4j.Logger log = LoggerFactory.getLogger(SolrLogFormatter.class);
    log.error("HELLO");
    
    ThreadGroup tg = new MyThreadGroup("YCS");
        
    Thread th = new Thread(tg, "NEW_THREAD") {

      @Override
      public void run() {
        try {
          go();
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
    
    th.start();
    th.join();
  }
  

  static class MyThreadGroup extends ThreadGroup implements TG {
    public MyThreadGroup(String name) {
      super(name);
    }
    public String getTag() { return "HELLO"; }
  }
  
  public static void go() throws Exception {
    final org.slf4j.Logger log = LoggerFactory.getLogger(SolrLogFormatter.class);
 
    Thread thread1 = new Thread() {
      @Override
      public void run() {
        threadLocal.set("from thread1");
        log.error("[] webapp=/solr path=/select params={hello} wow");
      }
    };

    Thread thread2 = new Thread() {
      @Override
      public void run() {
        threadLocal.set("from thread2");
        log.error("InThread2");
      }
    };

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();    
  }
}
