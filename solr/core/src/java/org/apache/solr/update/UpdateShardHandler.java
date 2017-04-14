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
package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.InstrumentedExecutorService;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.http.util.Args;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpClient;
import org.apache.solr.util.stats.InstrumentedPoolingClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;

public class UpdateShardHandler implements SolrMetricProducer, SolrInfoMBean {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * A downside to configuring an upper bound will be big update reorders (when that upper bound is hit)
   * and then undetected shard inconsistency as a result.
   * This update executor is used for different things too... both update streams (which may be very long lived)
   * and control messages (peersync? LIR?) and could lead to starvation if limited.
   * Therefore this thread pool is left unbounded. See SOLR-8205
   */
  private ExecutorService updateExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new SolrjNamedThreadFactory("updateExecutor"));
  
  private ExecutorService recoveryExecutor;
  
  private final CloseableHttpClient client;

  private final InstrumentedPoolingClientConnectionManager clientConnectionManager;

  private final UpdateShardHandlerConfig cfg;

  private IdleConnectionsEvictor idleConnectionsEvictor;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {
    this.cfg = cfg;
    clientConnectionManager = new InstrumentedPoolingClientConnectionManager(SchemeRegistryFactory.createSystemDefault());
    if (cfg != null ) {
      clientConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
      clientConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
    }

    ModifiableSolrParams clientParams = getClientParams();
    log.info("Creating UpdateShardHandler HTTP client with params: {}", clientParams);
    HttpClientMetricNameStrategy metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
    if (cfg != null)  {
      metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
      if (metricNameStrategy == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    InstrumentedHttpClient httpClient = new InstrumentedHttpClient(clientConnectionManager, metricNameStrategy);
    HttpClientUtil.configureClient(httpClient, clientParams);
    client = httpClient;
    if (cfg != null)  {
      idleConnectionsEvictor = new IdleConnectionsEvictor(clientConnectionManager,
          cfg.getUpdateConnectionsEvictorSleepDelay(), TimeUnit.MILLISECONDS,
          cfg.getMaxUpdateConnectionIdleTime(), TimeUnit.MILLISECONDS);
      idleConnectionsEvictor.start();
    }
    log.trace("Created UpdateShardHandler HTTP client with params: {}", clientParams);

    ThreadFactory recoveryThreadFactory = new SolrjNamedThreadFactory("recoveryExecutor");
    if (cfg != null && cfg.getMaxRecoveryThreads() > 0) {
      log.debug("Creating recoveryExecutor with pool size {}", cfg.getMaxRecoveryThreads());
      recoveryExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(cfg.getMaxRecoveryThreads(), recoveryThreadFactory);
    } else {
      log.debug("Creating recoveryExecutor with unbounded pool");
      recoveryExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(recoveryThreadFactory);
    }
  }

  protected ModifiableSolrParams getClientParams() {
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null) {
      clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT,
          cfg.getDistributedSocketTimeout());
      clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT,
          cfg.getDistributedConnectionTimeout());
    }
    // in the update case, we want to do retries, and to use
    // the default Solr retry handler that createClient will
    // give us
    clientParams.set(HttpClientUtil.PROP_USE_RETRY, true);
    return clientParams;
  }



  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String scope) {
    String expandedScope = SolrMetricManager.mkName(scope, getCategory().name());
    clientConnectionManager.initializeMetrics(manager, registry, expandedScope);
    if (client instanceof SolrMetricProducer) {
      SolrMetricProducer solrMetricProducer = (SolrMetricProducer) client;
      solrMetricProducer.initializeMetrics(manager, registry, expandedScope);
    }
    updateExecutor = new InstrumentedExecutorService(updateExecutor,
        manager.registry(registry),
        SolrMetricManager.mkName("updateExecutor", expandedScope, "threadPool"));
    recoveryExecutor = new InstrumentedExecutorService(recoveryExecutor,
        manager.registry(registry),
        SolrMetricManager.mkName("recoveryExecutor", expandedScope, "threadPool"));
  }

  @Override
  public String getDescription() {
    return "Metrics tracked by UpdateShardHandler related to distributed updates and recovery";
  }

  @Override
  public Category getCategory() {
    return Category.UPDATE;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return new URL[0];
  }

  @Override
  public NamedList getStatistics() {
    return null;
  }

  public HttpClient getHttpClient() {
    return client;
  }

  public void reconfigureHttpClient(HttpClientConfigurer configurer) {
    log.info("Reconfiguring the default client with: " + configurer);
    configurer.configure((DefaultHttpClient)client, getClientParams());
  }

  public ClientConnectionManager getConnectionManager() {
    return clientConnectionManager;
  }

  /**
   * This method returns an executor that is not meant for disk IO and that will
   * be interrupted on shutdown.
   * 
   * @return an executor for update related activities that do not do disk IO.
   */
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }

  /**
   * In general, RecoveryStrategy threads do not do disk IO, but they open and close SolrCores
   * in async threads, among other things, and can trigger disk IO, so we use this alternate 
   * executor rather than the 'updateExecutor', which is interrupted on shutdown.
   * 
   * @return executor for {@link RecoveryStrategy} thread which will not be interrupted on close.
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }

  public void close() {
    try {
      // do not interrupt, do not interrupt
      ExecutorUtil.shutdownAndAwaitTermination(updateExecutor);
      ExecutorUtil.shutdownAndAwaitTermination(recoveryExecutor);
      if (idleConnectionsEvictor != null) {
        idleConnectionsEvictor.shutdown();
      }
    } catch (Exception e) {
      SolrException.log(log, e);
    } finally {
      IOUtils.closeQuietly(client);
      clientConnectionManager.shutdown();
    }
  }

  /**
   * This class is adapted from org.apache.http.impl.client.IdleConnectionEvictor and changed to use
   * the deprecated ClientConnectionManager instead of the new HttpClientConnectionManager.
   * <p>
   * This class maintains a background thread to enforce an eviction policy for expired / idle
   * persistent connections kept alive in the connection pool.
   * <p>
   * See SOLR-9290 for related discussion.
   */
  public static final class IdleConnectionsEvictor {

    private final ClientConnectionManager connectionManager;
    private final ThreadFactory threadFactory;
    private final Thread thread;
    private final long sleepTimeMs;
    private final long maxIdleTimeMs;

    private volatile Exception exception;

    public IdleConnectionsEvictor(
        final ClientConnectionManager connectionManager,
        final ThreadFactory threadFactory,
        final long sleepTime, final TimeUnit sleepTimeUnit,
        final long maxIdleTime, final TimeUnit maxIdleTimeUnit) {
      this.connectionManager = Args.notNull(connectionManager, "Connection manager");
      this.threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory();
      this.sleepTimeMs = sleepTimeUnit != null ? sleepTimeUnit.toMillis(sleepTime) : sleepTime;
      this.maxIdleTimeMs = maxIdleTimeUnit != null ? maxIdleTimeUnit.toMillis(maxIdleTime) : maxIdleTime;
      this.thread = this.threadFactory.newThread(new Runnable() {
        @Override
        public void run() {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              Thread.sleep(sleepTimeMs);
              connectionManager.closeExpiredConnections();
              if (maxIdleTimeMs > 0) {
                connectionManager.closeIdleConnections(maxIdleTimeMs, TimeUnit.MILLISECONDS);
              }
            }
          } catch (Exception ex) {
            exception = ex;
          }

        }
      });
    }

    public IdleConnectionsEvictor(ClientConnectionManager connectionManager,
                                  long sleepTime, TimeUnit sleepTimeUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      this(connectionManager, null, sleepTime, sleepTimeUnit, maxIdleTime, maxIdleTimeUnit);
    }

    public void start() {
      thread.start();
    }

    public void shutdown() {
      thread.interrupt();
    }

    public boolean isRunning() {
      return thread.isAlive();
    }

    public void awaitTermination(final long time, final TimeUnit tunit) throws InterruptedException {
      thread.join((tunit != null ? tunit : TimeUnit.MILLISECONDS).toMillis(time));
    }

    static class DefaultThreadFactory implements ThreadFactory {

      @Override
      public Thread newThread(final Runnable r) {
        final Thread t = new Thread(r, "solr-idle-connections-evictor");
        t.setDaemon(true);
        return t;
      }

    }

  }

}
