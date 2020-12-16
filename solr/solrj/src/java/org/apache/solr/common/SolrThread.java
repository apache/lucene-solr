package org.apache.solr.common;


import java.util.concurrent.ExecutorService;

public class SolrThread extends Thread {

  private ExecutorService executorService;

  public SolrThread(ThreadGroup group, Runnable r, String name) {
    super(group, r, name);

//    Thread createThread = Thread.currentThread();
//    if (createThread instanceof SolrThread) {
//      // nocommit - disabled for now
//      ExecutorService service = null;//((SolrThread) createThread).getExecutorService();
//      if (service == null) {
//        createExecutorService();
//      } else {
//        setExecutorService(service);
//      }
//    }

  }

  public void run() {
    try {
      super.run();
    } finally {
//      ExecutorUtil.shutdownAndAwaitTermination(executorService);
//      executorService = null;
    }
  }

  private void setExecutorService(ExecutorService service) {
    this.executorService = service;
  }

  private void createExecutorService() {
    Integer minThreads;
    Integer maxThreads;
    minThreads = 4;
    maxThreads = ParWork.PROC_COUNT;
    this.executorService = ParWork.getExecutorService(Math.max(minThreads, maxThreads));
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public static SolrThread getCurrentThread() {
    return (SolrThread) currentThread();
  }

  public interface CreateThread  {
     SolrThread getCreateThread();
  }
}
