package org.apache.solr.servlet;

import org.apache.solr.common.ParWork;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SolrLifcycleListener extends AbstractLifeCycle.AbstractLifeCycleListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final static Set<Runnable> shutdowns = ConcurrentHashMap.newKeySet();

  private final static Set<Runnable> stopped = ConcurrentHashMap.newKeySet();

  public static void registerShutdown(Runnable r) {
    shutdowns.add(r);
  }

  public synchronized static void removeShutdown(Runnable r) {
    if (r == null) return;
    shutdowns.remove(r);
  }

  public synchronized static boolean isRegistered(Runnable r) {
    if (r == null) return false;
    return shutdowns.contains(r);
  }

  public synchronized static void registerStopped(Runnable r) {
    if (r == null) return;
    stopped.add(r);
  }

  public synchronized static void removeStopped(Runnable r) {
    if (r == null) return;
    stopped.remove(r);
  }

  public synchronized static boolean isRegisteredStopped(Runnable r) {
    if (r == null) return false;
    return stopped.contains(r);
  }

  @Override
  public void lifeCycleStopping(LifeCycle event) {
    log.info("Solr is stopping, call ZkController#disconnect");
    try (ParWork work = new ParWork(this, true, false)) {
      for (Runnable run : shutdowns) {
        work.collect("shutdown", () -> run.run());
      }
    }
    shutdowns.clear();
  }

  @Override
  public void lifeCycleStopped(LifeCycle event) {
    log.info("Solr is stopped, call shutdown");
    try (ParWork work = new ParWork(this, true, false)) {
      for (Runnable run : stopped) {
        work.collect("stopped", () -> run.run());
      }
    }
    stopped.clear();
  }
}
