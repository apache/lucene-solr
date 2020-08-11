package org.apache.solr.common.util;

import org.apache.solr.common.ParWork;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SysStats extends Thread {
    public static final int REFRESH_INTERVAL = 10000;
    static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

    private long refreshInterval;
    private  volatile boolean stopped;

    private volatile Map<Long, ThreadTime> threadTimeMap = new ConcurrentHashMap<>(512);
    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private OperatingSystemMXBean opBean = ManagementFactory.getOperatingSystemMXBean();

    private static volatile SysStats sysStats;

    public static SysStats getSysStats() {
        if (sysStats == null) {
          synchronized (SysStats.class) {
            if (sysStats == null) {
              sysStats = new SysStats(REFRESH_INTERVAL);
            }
          }
        }
        return  sysStats;
    }

    public SysStats(long refreshInterval) {
        this.refreshInterval = refreshInterval;
        setName("CPUMonitoringThread");
        setDaemon(true);
        start();
    }

    public static synchronized void reStartSysStats() {
        if (sysStats != null) {
            sysStats.stopMonitor();
        }
        sysStats = new SysStats(REFRESH_INTERVAL);
    }

    public void doStop() {
        this.interrupt();
        this.stopped = true;
    }

    @Override
    public void run() {
        while(!stopped) {
            Set<Long> mappedIds = new HashSet<Long>(threadTimeMap.keySet());


            long[] allThreadIds = threadBean.getAllThreadIds();

            removeDeadThreads(mappedIds, allThreadIds);

            mapNewThreads(allThreadIds);

            Collection<ThreadTime> values = new HashSet<ThreadTime>(threadTimeMap.values());


            for (ThreadTime threadTime : values) {
              threadTime.setCurrent(threadBean.getThreadCpuTime(threadTime.getId()));
            }

            try {
                Thread.sleep(refreshInterval);
                if (stopped) {
                    return;
                }
            } catch (InterruptedException e) {
                ParWork.propegateInterrupt(e, true);
                return;
            }

            for (ThreadTime threadTime : values) {
              threadTime.setLast(threadTime.getCurrent());
            }

        }
    }

    private void mapNewThreads(long[] allThreadIds) {
      for (long id : allThreadIds) {
        if (!threadTimeMap.containsKey(id))
          threadTimeMap.put(id, new ThreadTime(id));
      }
    }

    private void removeDeadThreads(Set<Long> mappedIds, long[] allThreadIds) {
        outer: for (long id1 : mappedIds) {
            for (long id2 : allThreadIds) {
                if(id1 == id2)
                    continue outer;
            }
            threadTimeMap.remove(id1);
        }
    }

    public void stopMonitor() {
        this.stopped = true;
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public double getTotalUsage() {
        Collection<ThreadTime> values;
        values = new HashSet<ThreadTime>(threadTimeMap.values());

        double usage = 0D;
        for (ThreadTime threadTime : values) {
          usage += (threadTime.getCurrent() - threadTime.getLast()) / (refreshInterval * REFRESH_INTERVAL);
        }
        return usage;
    }

    public double getAvarageUsagePerCPU() {
        return getTotalUsage() / ParWork.PROC_COUNT;
    }

    public double getUsageByThread(Thread t) {
        ThreadTime info;

        info = threadTimeMap.get(t.getId());

        double usage = 0D;
        if(info != null) {
            synchronized (info) {
                usage = (info.getCurrent() - info.getLast()) / (TimeUnit.MILLISECONDS.toNanos(refreshInterval));
            }
        }
        return usage;
    }

    static class ThreadTime {

        private volatile long id;
        private volatile long last;
        private volatile long current;

        public ThreadTime(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public long getLast() {
            return last;
        }

        public void setLast(long last) {
            this.last = last;
        }

        public long getCurrent() {
            return current;
        }

        public void setCurrent(long current) {
            this.current = current;
        }
    }
}