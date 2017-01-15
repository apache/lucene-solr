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
package org.apache.solr.update.processor;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a chain of UpdateRequestProcessorFactories.
 * <p>
 * Chains can be configured via solrconfig.xml using the following syntax...
 * </p>
 * <pre class="prettyprint">
 * &lt;updateRequestProcessorChain name="key" default="true"&gt;
 *   &lt;processor class="package.Class1" /&gt;
 *   &lt;processor class="package.Class2" &gt;
 *     &lt;str name="someInitParam1"&gt;value&lt;/str&gt;
 *     &lt;int name="someInitParam2"&gt;42&lt;/int&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.LogUpdateProcessorFactory" &gt;
 *     &lt;int name="maxNumToLog"&gt;100&lt;/int&gt;
 *   &lt;/processor&gt;
 *   &lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 * &lt;/updateRequestProcessorChain&gt;
 * </pre>
 * <p>
 * Multiple Chains can be defined, each with a distinct name.  The name of 
 * a chain used to handle an update request may be specified using the request 
 * param <code>update.chain</code>.  If no chain is explicitly selected 
 * by name, then Solr will attempt to determine a default chain:
 * </p>
 * <ul>
 *  <li>A single configured chain may explicitly be declared with 
 *      <code>default="true"</code> (see example above)</li>
 *  <li>If no chain is explicitly declared as the default, Solr will look for
 *      any chain that does not have a name, and treat it as the default</li>
 *  <li>As a last resort, Solr will create an implicit default chain 
 *      consisting of:<ul>
 *        <li>{@link LogUpdateProcessorFactory}</li>
 *        <li>{@link DistributedUpdateProcessorFactory}</li>
 *        <li>{@link RunUpdateProcessorFactory}</li>
 *      </ul></li>
 * </ul>
 *
 * <p>
 * Almost all processor chains should end with an instance of 
 * <code>RunUpdateProcessorFactory</code> unless the user is explicitly 
 * executing the update commands in an alternative custom 
 * <code>UpdateRequestProcessorFactory</code>.  If a chain includes 
 * <code>RunUpdateProcessorFactory</code> but does not include a 
 * <code>DistributingUpdateProcessorFactory</code>, it will be added 
 * automatically by {@link #init init()}.
 * </p>
 *
 * @see UpdateRequestProcessorFactory
 * @see #init
 * @see #createProcessor
 * @since solr 1.3
 */
public final class UpdateRequestProcessorChain implements PluginInfoInitialized
{
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private List<UpdateRequestProcessorFactory> chain;
  private final SolrCore solrCore;

  public UpdateRequestProcessorChain(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  /**
   * Initializes the chain using the factories specified by the <code>PluginInfo</code>.
   * if the chain includes the <code>RunUpdateProcessorFactory</code>, but
   * does not include an implementation of the 
   * <code>DistributingUpdateProcessorFactory</code> interface, then an 
   * instance of <code>DistributedUpdateProcessorFactory</code> will be 
   * injected immediately prior to the <code>RunUpdateProcessorFactory</code>.
   *
   * @see DistributingUpdateProcessorFactory
   * @see RunUpdateProcessorFactory
   * @see DistributedUpdateProcessorFactory
   */
  @Override
  public void init(PluginInfo info) {
    final String infomsg = "updateRequestProcessorChain \"" + 
      (null != info.name ? info.name : "") + "\"" + 
      (info.isDefault() ? " (default)" : "");

    log.debug("creating " + infomsg);

    // wrap in an ArrayList so we know we know we can do fast index lookups 
    // and that add(int,Object) is supported
    List<UpdateRequestProcessorFactory> list = new ArrayList<>
      (solrCore.initPlugins(info.getChildren("processor"),UpdateRequestProcessorFactory.class,null));

    if(list.isEmpty()){
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              infomsg + " require at least one processor");
    }

    int numDistrib = 0;
    int runIndex = -1;
    // hi->lo incase multiple run instances, add before first one
    // (no idea why someone might use multiple run instances, but just in case)
    for (int i = list.size()-1; 0 <= i; i--) {
      UpdateRequestProcessorFactory factory = list.get(i);
      if (factory instanceof DistributingUpdateProcessorFactory) {
        numDistrib++;
      }
      if (factory instanceof RunUpdateProcessorFactory) {
        runIndex = i;
      }
    }
    if (1 < numDistrib) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              infomsg + " may not contain more then one " +
                              "instance of DistributingUpdateProcessorFactory");
    }
    if (0 <= runIndex && 0 == numDistrib) {
      // by default, add distrib processor immediately before run
      DistributedUpdateProcessorFactory distrib
        = new DistributedUpdateProcessorFactory();
      distrib.init(new NamedList());
      list.add(runIndex, distrib);

      log.debug("inserting DistributedUpdateProcessorFactory into " + infomsg);
    }

    chain = list;
    ProcessorInfo processorInfo = new ProcessorInfo(new MapSolrParams(info.attributes));
    if (processorInfo.isEmpty()) return;
    UpdateRequestProcessorChain newChain = constructChain(this, processorInfo, solrCore);
    chain = newChain.chain;

  }

  /**
   * Creates a chain backed directly by the specified list. Modifications to
   * the array will affect future calls to <code>createProcessor</code>
   */
  public UpdateRequestProcessorChain(List<UpdateRequestProcessorFactory> chain,
                                      SolrCore solrCore) {
    this.chain = chain;
    this.solrCore =  solrCore;
  }


  /**
   * Uses the factories in this chain to creates a new 
   * <code>UpdateRequestProcessor</code> instance specific for this request.  
   * If the <code>DISTRIB_UPDATE_PARAM</code> is present in the request and is 
   * non-blank, then any factory in this chain prior to the instance of 
   * <code>{@link DistributingUpdateProcessorFactory}</code> will be skipped, 
   * except for the log update processor factory.
   *
   * @see UpdateRequestProcessorFactory#getInstance
   * @see DistributingUpdateProcessorFactory#DISTRIB_UPDATE_PARAM
   */
  public UpdateRequestProcessor createProcessor(SolrQueryRequest req, 
                                                SolrQueryResponse rsp) 
  {
    UpdateRequestProcessor processor = null;
    UpdateRequestProcessor last = null;
    
    final String distribPhase = req.getParams().get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    final boolean skipToDistrib = distribPhase != null;
    boolean afterDistrib = true;  // we iterate backwards, so true to start

    for (int i = chain.size() - 1; i >= 0; i--) {
      UpdateRequestProcessorFactory factory = chain.get(i);

      if (skipToDistrib) {
        if (afterDistrib) {
          if (factory instanceof DistributingUpdateProcessorFactory) {
            afterDistrib = false;
          }
        } else if (!(factory instanceof UpdateRequestProcessorFactory.RunAlways)) {
          // skip anything that doesn't have the marker interface
          continue;
        }
      }

      processor = factory.getInstance(req, rsp, last);
      last = processor == null ? last : processor;
    }

    return last;
  }

  /**
   * Returns the underlying array of factories used in this chain.
   * Modifications to the array will affect future calls to
   * <code>createProcessor</code>
   */
  public List<UpdateRequestProcessorFactory> getProcessors() {
    return chain;
  }

  public static UpdateRequestProcessorChain constructChain(UpdateRequestProcessorChain defaultUrp,
                                                           ProcessorInfo processorInfo, SolrCore core) {
    LinkedList<UpdateRequestProcessorFactory> urps = new LinkedList<>(defaultUrp.chain);
    List<UpdateRequestProcessorFactory> p = getReqProcessors(processorInfo.processor, core);
    List<UpdateRequestProcessorFactory> post = getReqProcessors(processorInfo.postProcessor, core);
    //processor are tried to be inserted before LogUpdateprocessor+DistributedUpdateProcessor
    insertBefore(urps, p, DistributedUpdateProcessorFactory.class, 0);
    //port-processor is tried to be inserted before RunUpdateProcessor
    insertBefore(urps, post, RunUpdateProcessorFactory.class, urps.size() - 1);
    UpdateRequestProcessorChain result = new UpdateRequestProcessorChain(urps, core);
    if (log.isInfoEnabled()) {
      ArrayList<String> names = new ArrayList<>(urps.size());
      for (UpdateRequestProcessorFactory urp : urps) names.add(urp.getClass().getSimpleName());
      log.debug("New dynamic chain constructed : " + StrUtils.join(names, '>'));
    }
    return result;
  }

  private static void insertBefore(LinkedList<UpdateRequestProcessorFactory> urps, List<UpdateRequestProcessorFactory> newFactories, Class klas, int idx) {
    if (newFactories.isEmpty()) return;
    for (int i = 0; i < urps.size(); i++) {
      if (klas.isInstance(urps.get(i))) {
        idx = i;
        if (klas == DistributedUpdateProcessorFactory.class) {
          if (i > 0 && urps.get(i - 1) instanceof LogUpdateProcessorFactory) {
            idx = i - 1;
          }
        }
        break;
      }
    }
    for (int i = newFactories.size() - 1; 0 <= i; i--) urps.add(idx, newFactories.get(i));
  }

  static List<UpdateRequestProcessorFactory> getReqProcessors(String processor, SolrCore core) {
    if (processor == null) return Collections.emptyList();
    List<UpdateRequestProcessorFactory> result = new ArrayList<>();
    List<String> names = StrUtils.splitSmart(processor, ',');
    for (String s : names) {
      s = s.trim();
      if (s.isEmpty()) continue;
      UpdateRequestProcessorFactory p = core.getUpdateProcessors().get(s);
      if (p == null) {
        try {
          p = core.createInstance(s + "UpdateProcessorFactory", UpdateRequestProcessorFactory.class,
              "updateProcessor", null, core.getMemClassLoader());
          core.getUpdateProcessors().put(s, p);
        } catch (SolrException e) {
        }
        if (p == null)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such processor " + s);
      }
      result.add(p);
    }
    return result;
  }

  public static class ProcessorInfo {
    public final String processor, postProcessor;

    public ProcessorInfo(SolrParams params) {
      processor = params.get("processor");
      postProcessor = params.get("post-processor");
    }

    public boolean isEmpty() {
      return processor == null && postProcessor == null;
    }

    @Override
    public int hashCode() {
      int hash = 0;
      if (processor != null) hash += processor.hashCode();
      if (postProcessor != null) hash += postProcessor.hashCode();
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ProcessorInfo)) return false;
      ProcessorInfo that = (ProcessorInfo) obj;

      return Objects.equals(this.processor, that.processor) &&
          Objects.equals(this.postProcessor, that.postProcessor);
    }
  }

}
