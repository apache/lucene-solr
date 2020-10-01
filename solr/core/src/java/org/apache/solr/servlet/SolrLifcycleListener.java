package org.apache.solr.servlet;

import org.apache.solr.common.ParWork;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashSet;
import java.util.Set;

public class SolrLifcycleListener extends AbstractLifeCycle.AbstractLifeCycleListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final static Set<Runnable> shutdowns = new LinkedHashSet<>();


  public synchronized static void registerShutdown(Runnable r) {
    shutdowns.add(r);
  }

  public synchronized static void removeShutdown(Runnable r) {
    shutdowns.remove(r);
  }

  public synchronized static boolean isRegistered(Runnable r) {
    return shutdowns.contains(r);
  }


  @Override
  public void lifeCycleStopping(LifeCycle event) {
    log.info("Solr is stopping, call ZkController#disconnect");
    try (ParWork work = new ParWork(this, true)) {
      for (Runnable run : shutdowns) {
        work.collect("shutdown", () -> run.run());
      }
    }
    shutdowns.clear();
  }
}
