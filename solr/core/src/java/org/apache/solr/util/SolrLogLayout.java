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
package org.apache.solr.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.slf4j.MDC;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;

@SuppressForbidden(reason = "class is specific to log4j")
public class SolrLogLayout extends Layout {
  /**
   * Add this interface to a thread group and the string returned by getTag()
   * will appear in log statements of any threads under that group.
   */
  public static interface TG {
    public String getTag();
  }

  @SuppressForbidden(reason = "Need currentTimeMillis to compare against log event timestamp. " +
    "This is inaccurate but unavoidable due to interface limitations, in any case this is just for logging.")
  final long startTime = System.currentTimeMillis();

  long lastTime = startTime;
  Map<Method,String> methodAlias = new HashMap<>();
  
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
      if (!(obj instanceof Method)) return false;
      Method other = (Method) obj;
      return (className.equals(other.className) && methodName
          .equals(other.methodName));
    }
    
    @Override
    public String toString() {
      return className + '.' + methodName;
    }
  }
  
  public static class CoreInfo {
    static int maxCoreNum;
    String shortId;
    String url;
    Map<String,Object> coreProps;
  }
  
  Map<Integer,CoreInfo> coreInfoMap = new WeakHashMap<>();
  
  public Map<String,String> classAliases = new HashMap<>();
  
  public void appendThread(StringBuilder sb, LoggingEvent event) {
    Thread th = Thread.currentThread();
    
    /******
     * sb.append(" T="); sb.append(th.getName()).append(' ');
     * 
     * // NOTE: tried creating a thread group around jetty but we seem to lose
     * it and request // threads are in the normal "main" thread group
     * ThreadGroup tg = th.getThreadGroup(); while (tg != null) {
     * sb.append("(group_name=").append(tg.getName()).append(")");
     * 
     * if (tg instanceof TG) { sb.append(((TG)tg).getTag()); sb.append('/'); }
     * try { tg = tg.getParent(); } catch (Throwable e) { tg = null; } }
     ******/
    
    // NOTE: LogRecord.getThreadID is *not* equal to Thread.getId()
    sb.append(" T");
    sb.append(th.getId());
  }

  @Override
  public String format(LoggingEvent event) {
    return _format(event);
  }
  
  public String _format(LoggingEvent event) {
    String message = (String) event.getMessage();
    if (message == null) {
      message = "";
    }
    StringBuilder sb = new StringBuilder(message.length() + 80);
    
    long now = event.timeStamp;
    long timeFromStart = now - startTime;
    long timeSinceLast = now - lastTime;
    lastTime = now;
    String shortClassName = getShortClassName(event.getLocationInformation().getClassName(),
        event.getLocationInformation().getMethodName());
    
    /***
     * sb.append(timeFromStart).append(' ').append(timeSinceLast);
     * sb.append(' ');
     * sb.append(record.getSourceClassName()).append('.').append(
     * record.getSourceMethodName()); sb.append(' ');
     * sb.append(record.getLevel());
     ***/
    
    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    SolrQueryRequest req = requestInfo == null ? null : requestInfo.getReq();
    SolrCore core = req == null ? null : req.getCore();
    ZkController zkController = null;
    CoreInfo info = null;
    
    if (core != null) {
      info = coreInfoMap.get(core.hashCode());
      if (info == null) {
        info = new CoreInfo();
        info.shortId = "C" + Integer.toString(CoreInfo.maxCoreNum++);
        coreInfoMap.put(core.hashCode(), info);
        
        if (sb.length() == 0) sb.append("ASYNC ");
        sb.append(" NEW_CORE " + info.shortId);
        sb.append(" name=" + core.getName());
        sb.append(" " + core);
      }

      zkController = core.getCoreContainer().getZkController();
      if (zkController != null) {
        if (info.url == null) {
          info.url = zkController.getBaseUrl() + "/" + core.getName();
          sb.append(" url=" + info.url + " node=" + zkController.getNodeName());
        }
        
        Map<String,Object> coreProps = getReplicaProps(zkController, core);
        if (info.coreProps == null || !coreProps.equals(info.coreProps)) {
          info.coreProps = coreProps;
          final String corePropsString = "coll:"
              + core.getCoreDescriptor().getCloudDescriptor()
                  .getCollectionName() + " core:" + core.getName() + " props:"
              + coreProps;
          sb.append(" " + info.shortId + "_STATE=" + corePropsString);
        }
      }
    }
    
    if (sb.length() > 0) sb.append('\n');
    sb.append(timeFromStart);

    // sb.append("\nL").append(record.getSequenceNumber()); // log number is
    // useful for sequencing when looking at multiple parts of a log file, but
    // ms since start should be fine.
    appendThread(sb, event);

    appendMDC(sb);

    // todo: should be able to get port from core container for non zk tests

    if (info != null) {
      sb.append(' ').append(info.shortId); // core
    }

    if (shortClassName.length() > 0) {
      sb.append(' ').append(shortClassName);
    }

    if (event.getLevel() != Level.INFO) {
      sb.append(' ').append(event.getLevel());
    }
    
    sb.append(' ');
    appendMultiLineString(sb, message);
    ThrowableInformation thInfo = event.getThrowableInformation();
    if (thInfo != null) {
      Throwable th = event.getThrowableInformation().getThrowable();
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
    }
    
    sb.append('\n');
    
    /***
     * Isn't core specific... prob better logged from zkController if (info !=
     * null) { ClusterState clusterState = zkController.getClusterState(); if
     * (info.clusterState != clusterState) { // something has changed in the
     * matrix... sb.append(zkController.getBaseUrl() +
     * " sees new ClusterState:"); } }
     ***/
    
    return sb.toString();
  }

  private Map<String, Object> getReplicaProps(ZkController zkController, SolrCore core) {
    final String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    DocCollection collection = zkController.getClusterState().getCollectionOrNull(collectionName);
    Replica replica = collection.getReplica(zkController.getCoreNodeName(core.getCoreDescriptor()));
    if (replica != null) {
      return replica.getProperties();
    }
    return Collections.EMPTY_MAP;
  }
  
  private void addFirstLine(StringBuilder sb, String msg) {
    // INFO: [] webapp=/solr path=/select params={q=foobarbaz} hits=0 status=0
    // QTime=1
    
    if (!shorterFormat || !msg.startsWith("[")) {
      sb.append(msg);
      return;
    }
    
    int idx = msg.indexOf(']');
    if (idx < 0 || !msg.startsWith(" webapp=", idx + 1)) {
      sb.append(msg);
      return;
    }
    
    idx = msg.indexOf(' ', idx + 8); // space after webapp=
    if (idx < 0) {
      sb.append(msg);
      return;
    }
    idx = msg.indexOf('=', idx + 1); // = in path=
    if (idx < 0) {
      sb.append(msg);
      return;
    }
    
    int idx2 = msg.indexOf(' ', idx + 1);
    if (idx2 < 0) {
      sb.append(msg);
      return;
    }
    
    sb.append(msg.substring(idx + 1, idx2 + 1)); // path
    
    idx = msg.indexOf("params=", idx2);
    if (idx < 0) {
      sb.append(msg.substring(idx2));
    } else {
      sb.append(msg.substring(idx + 7));
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
          addFirstLine(sb, msg.substring(lastIdx + 1));
        } else {
          sb.append(msg.substring(lastIdx + 1));
        }
        break;
      }
      if (lastIdx == -1) {
        addFirstLine(sb, msg.substring(lastIdx + 1, idx));
      } else {
        sb.append(msg.substring(lastIdx + 1, idx));
      }
      
      sb.append("\n\t");
      lastIdx = idx;
      idx = msg.indexOf('\n', lastIdx + 1);
    }
  }
  
  // TODO: name this better... it's only for cloud tests where every core
  // container has just one solr server so Port/Core are fine
  public boolean shorterFormat = false;
  
  public void setShorterFormat() {
    shorterFormat = true;
    // looking at /update is enough... we don't need "UPDATE /update"
    methodAlias.put(new Method(
        "org.apache.solr.update.processor.LogUpdateProcessor", "finish"), "");
  }
  
  private Method classAndMethod = new Method(null, null); // don't need to be
                                                          // thread safe
  
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
      int idx = name.indexOf('.', prevIndex + 1);
      ch = name.charAt(idx + 1);
      if (idx >= lastDot || Character.isUpperCase(ch)) {
        sb.append(name.substring(idx));
        break;
      }
      prevIndex = idx;
    }
    
    return sb.toString() + '.' + method;
  }

  @Override
  public void activateOptions() {}

  @Override
  public boolean ignoresThrowable() {
    return false;
  }


  private void appendMDC(StringBuilder sb) {
    if (!StringUtils.isEmpty(MDC.get(NODE_NAME_PROP)))  {
      sb.append(" n:").append(MDC.get(NODE_NAME_PROP));
    }
    if (!StringUtils.isEmpty(MDC.get(COLLECTION_PROP)))  {
      sb.append(" c:").append(MDC.get(COLLECTION_PROP));
    }
    if (!StringUtils.isEmpty(MDC.get(SHARD_ID_PROP))) {
      sb.append(" s:").append(MDC.get(SHARD_ID_PROP));
    }
    if (!StringUtils.isEmpty(MDC.get(REPLICA_PROP))) {
      sb.append(" r:").append(MDC.get(REPLICA_PROP));
    }
    if (!StringUtils.isEmpty(MDC.get(CORE_NAME_PROP))) {
      sb.append(" x:").append(MDC.get(CORE_NAME_PROP));
    }
  }
}
