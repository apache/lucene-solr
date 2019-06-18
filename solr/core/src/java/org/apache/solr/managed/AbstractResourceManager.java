package org.apache.solr.managed;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class AbstractResourceManager implements ResourceManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SCHEDULE_DELAY_SECONDS_PARAM = "scheduleDelaySeconds";
  public static final String MAX_NUM_POOLS_PARAM = "maxNumPools";

  public static final int DEFAULT_MAX_POOLS = 20;

  public static class Pool implements Runnable, Closeable {
    private final AbstractResourceManager resourceManager;
    private final Map<String, ResourceManaged> resources = new ConcurrentHashMap<>();
    private Limits limits;
    private final Map<String, Object> params;
    private final Map<String, Float> totalCosts = new ConcurrentHashMap<>();
    private Map<String, Map<String, Float>> currentValues = null;
    private Map<String, Float> totalValues = null;
    int scheduleDelaySeconds;
    ScheduledFuture<?> scheduledFuture;

    public Pool(AbstractResourceManager resourceManager, Limits limits, Map<String, Object> params) {
      this.resourceManager = resourceManager;
      this.limits = limits.copy();
      this.params = new HashMap<>(params);
    }

    public synchronized void addResource(ResourceManaged resourceManaged) {
      if (resources.containsKey(resourceManaged.getName())) {
        throw new IllegalArgumentException("Pool already has resource '" + resourceManaged.getName() + "'.");
      }
      resources.put(resourceManaged.getName(), resourceManaged);
      Limits managedLimits = resourceManaged.getManagedLimits();
      managedLimits.forEach(entry -> {
        Float total = totalCosts.get(entry.getKey());
        if (total == null) {
          totalCosts.put(entry.getKey(), entry.getValue().cost);
        } else {
          totalCosts.put(entry.getKey(), entry.getValue().cost + total);
        }
      });
    }

    public Map<String, ResourceManaged> getResources() {
      return Collections.unmodifiableMap(resources);
    }

    public Map<String, Map<String, Float>> getCurrentValues() {
      // collect current values
      currentValues = new HashMap<>();
      for (ResourceManaged resource : resources.values()) {
        currentValues.put(resource.getName(), resource.getManagedValues());
      }
      // calculate totals
      totalValues = new HashMap<>();
      currentValues.values().forEach(map -> map.forEach((k, v) -> {
        Float total = totalValues.get(k);
        if (total == null) {
          totalValues.put(k, v);
        } else {
          totalValues.put(k, total + v);
        }
      }));
      return Collections.unmodifiableMap(currentValues);
    }

    /**
     * This returns cumulative values of all resources. NOTE:
     * you must call {@link #getCurrentValues()} first!
     * @return
     */
    public Map<String, Float> getTotalValues() {
      return Collections.unmodifiableMap(totalValues);
    }

    public Map<String, Float> getTotalCosts() {
      return Collections.unmodifiableMap(totalCosts);
    }

    public Limits getLimits() {
      return limits;
    }

    public void setLimits(Limits limits) {
      this.limits = limits.copy();
    }

    @Override
    public void run() {
      resourceManager.managePool(this);
    }

    @Override
    public void close() throws IOException {
      if (scheduledFuture != null) {
        scheduledFuture.cancel(true);
        scheduledFuture = null;
      }
    }
  }


  private Map<String, Pool> resourcePools = new ConcurrentHashMap<>();
  private PluginInfo pluginInfo;
  private int maxNumPools = DEFAULT_MAX_POOLS;
  private TimeSource timeSource;

  /**
   * Thread pool for scheduling the pool runs.
   */
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  protected boolean isClosed = false;
  protected boolean enabled = true;

  public AbstractResourceManager(TimeSource timeSource) {
    this.timeSource = timeSource;
  }

  @Override
  public void init(PluginInfo info) {
    if (info != null) {
      this.pluginInfo = info.copy();
      if (pluginInfo.initArgs != null) {
        SolrPluginUtils.invokeSetters(this, this.pluginInfo.initArgs);
      }
    }
    if (!enabled) {
      log.debug("Resource manager " + getClass().getSimpleName() + " disabled.");
      return;
    }
    scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(maxNumPools,
        new DefaultSolrThreadFactory(getClass().getSimpleName()));
    scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  public void setMaxNumPools(Integer maxNumPools) {
    if (maxNumPools != null) {
      this.maxNumPools = maxNumPools;
    } else {
      this.maxNumPools = DEFAULT_MAX_POOLS;
    }
  }

  @Override
  public void setEnabled(Boolean enabled) {
    if (enabled != null) {
      this.enabled = enabled;
    }
  }

  @Override
  public PluginInfo getPluginInfo() {
    return pluginInfo;
  }

  protected abstract void managePool(Pool pool);

  @Override
  public void createPool(String name, Limits limits, Map<String, Object> params) throws Exception {
    ensureNotClosed();
    if (resourcePools.containsKey(name)) {
      throw new IllegalArgumentException("Pool '" + name + "' already exists.");
    }
    if (resourcePools.size() >= maxNumPools) {
      throw new IllegalArgumentException("Maximum number of pools (" + maxNumPools + ") reached.");
    }
    Pool newPool = new Pool(this, limits, params);
    newPool.scheduleDelaySeconds = Integer.parseInt(String.valueOf(params.getOrDefault(SCHEDULE_DELAY_SECONDS_PARAM, 10)));
    resourcePools.putIfAbsent(name, newPool);
    newPool.scheduledFuture = scheduledThreadPoolExecutor.scheduleWithFixedDelay(newPool, 0,
        timeSource.convertDelay(TimeUnit.SECONDS, newPool.scheduleDelaySeconds, TimeUnit.MILLISECONDS),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void modifyPoolLimits(String name, Limits limits) throws Exception {
    ensureNotClosed();
    Pool pool = resourcePools.get(name);
    if (pool == null) {
      throw new IllegalArgumentException("Pool '" + name + "' doesn't exist.");
    }
    pool.setLimits(limits);
  }

  @Override
  public void removePool(String name) throws Exception {
    ensureNotClosed();
    Pool pool = resourcePools.remove(name);
    if (pool == null) {
      throw new IllegalArgumentException("Pool '" + name + "' doesn't exist.");
    }
    if (pool.scheduledFuture != null) {
      pool.scheduledFuture.cancel(true);
    }
  }

  @Override
  public void addResources(String name, Collection<ResourceManaged> resourceManaged) {
    ensureNotClosed();
    for (ResourceManaged resource : resourceManaged) {
      addResource(name, resource);
    }
  }

  @Override
  public void addResource(String name, ResourceManaged resourceManaged) {
    ensureNotClosed();
    Pool pool = resourcePools.get(name);
    if (pool == null) {
      throw new IllegalArgumentException("Pool '" + name + "' doesn't exist.");
    }
    pool.addResource(resourceManaged);
  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      isClosed = true;
      log.debug("Closing all pools.");
      for (Pool pool : resourcePools.values()) {
        IOUtils.closeQuietly(pool);
      }
      resourcePools.clear();
    }
    log.debug("Shutting down scheduled thread pool executor now");
    scheduledThreadPoolExecutor.shutdownNow();
    log.debug("Awaiting termination of scheduled thread pool executor");
    ExecutorUtil.awaitTermination(scheduledThreadPoolExecutor);
    log.debug("Closed.");
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
