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
package org.apache.solr.ltr;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * The LTRThreadModule is optionally used by the {@link org.apache.solr.ltr.search.LTRQParserPlugin} and
 * {@link org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory LTRFeatureLoggerTransformerFactory}
 * classes to parallelize the creation of {@link org.apache.solr.ltr.feature.Feature.FeatureWeight Feature.FeatureWeight}
 * objects.
 * <p>
 * Example configuration:
 * <pre>
  &lt;queryParser name="ltr" class="org.apache.solr.ltr.search.LTRQParserPlugin"&gt;
     &lt;int name="threadModule.totalPoolThreads"&gt;10&lt;/int&gt;
     &lt;int name="threadModule.numThreadsPerRequest"&gt;5&lt;/int&gt;
  &lt;/queryParser&gt;

  &lt;transformer name="features" class="org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory"&gt;
     &lt;int name="threadModule.totalPoolThreads"&gt;10&lt;/int&gt;
     &lt;int name="threadModule.numThreadsPerRequest"&gt;5&lt;/int&gt;
  &lt;/transformer&gt;
</pre>
 * If an individual solr instance is expected to receive no more than one query at a time, it is best
 * to set <code>totalPoolThreads</code> and <code>numThreadsPerRequest</code> to the same value.
 *
 * If multiple queries need to be serviced simultaneously then <code>totalPoolThreads</code> and
 * <code>numThreadsPerRequest</code> can be adjusted based on the expected response times.
 *
 * If the value of <code>numThreadsPerRequest</code> is higher, the response time for a single query
 * will be improved up to a point. If multiple queries are serviced simultaneously, the value of
 * <code>totalPoolThreads</code> imposes a contention between the queries if
 * <code>(totalPoolThreads &lt; numThreadsPerRequest * total parallel queries)</code>.
 */
final public class LTRThreadModule extends CloseHook implements NamedListInitializedPlugin  {

  public static LTRThreadModule getInstance(@SuppressWarnings({"rawtypes"})NamedList args) {

    final LTRThreadModule threadManager;
    @SuppressWarnings({"rawtypes"})
    final NamedList threadManagerArgs = extractThreadModuleParams(args);
    // if and only if there are thread module args then we want a thread module!
    if (threadManagerArgs.size() > 0) {
      // create and initialize the new instance
      threadManager = new LTRThreadModule();
      threadManager.init(threadManagerArgs);
    } else {
      threadManager = null;
    }

    return threadManager;
  }

  private static String CONFIG_PREFIX = "threadModule.";

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static NamedList extractThreadModuleParams(NamedList args) {

    // gather the thread module args from amongst the general args
    final NamedList extractedArgs = new NamedList();
    for (Iterator<Map.Entry<String,Object>> it = args.iterator();
        it.hasNext(); ) {
      final Map.Entry<String,Object> entry = it.next();
      final String key = entry.getKey();
      if (key.startsWith(CONFIG_PREFIX)) {
        extractedArgs.add(key.substring(CONFIG_PREFIX.length()), entry.getValue());
      }
    }

    // remove consumed keys only once iteration is complete
    // since NamedList iterator does not support 'remove'
    for (Object key : extractedArgs.asShallowMap().keySet()) {
      args.remove(CONFIG_PREFIX+key);
    }

    return extractedArgs;
  }

  // settings
  private int totalPoolThreads = 1;
  private int numThreadsPerRequest = 1;

  // implementation
  private Semaphore ltrSemaphore;
  private volatile ExecutorService createWeightScoreExecutor;

  public LTRThreadModule() {
  }

  // For test use only.
  LTRThreadModule(int totalPoolThreads, int numThreadsPerRequest) {
    this.totalPoolThreads = totalPoolThreads;
    this.numThreadsPerRequest = numThreadsPerRequest;
    init(null);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    if (args != null) {
      SolrPluginUtils.invokeSetters(this, args);
    }
    validate();
    if  (this.totalPoolThreads > 1 ){
      ltrSemaphore = new Semaphore(totalPoolThreads);
    } else {
      ltrSemaphore = null;
    }
  }

  private void validate() {
    if (totalPoolThreads <= 0){
      throw new IllegalArgumentException("totalPoolThreads cannot be less than 1");
    }
    if (numThreadsPerRequest <= 0){
      throw new IllegalArgumentException("numThreadsPerRequest cannot be less than 1");
    }
    if (totalPoolThreads < numThreadsPerRequest){
      throw new IllegalArgumentException("numThreadsPerRequest cannot be greater than totalPoolThreads");
    }
  }

  public void setTotalPoolThreads(int totalPoolThreads) {
    this.totalPoolThreads = totalPoolThreads;
  }

  public void setNumThreadsPerRequest(int numThreadsPerRequest) {
    this.numThreadsPerRequest = numThreadsPerRequest;
  }

  public Semaphore createQuerySemaphore() {
    return (numThreadsPerRequest > 1 ? new Semaphore(numThreadsPerRequest) : null);
  }

  public void acquireLTRSemaphore() throws InterruptedException {
    ltrSemaphore.acquire();
  }

  public void releaseLTRSemaphore() throws InterruptedException {
    ltrSemaphore.release();
  }

  public void execute(Runnable command) {
    createWeightScoreExecutor.execute(command);
  }

  @Override
  public void preClose(SolrCore core) {
    ExecutorUtil.shutdownAndAwaitTermination(createWeightScoreExecutor);
  }

  @Override
  public void postClose(SolrCore core) {
  
  }

  public void setExecutor(ExecutorService sharedExecutor) {
    this.createWeightScoreExecutor = sharedExecutor;
  }

}
