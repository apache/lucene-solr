package org.apache.solr.common.util;

import org.apache.solr.common.ParWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SysStats extends Thread {
    private static final Logger log = LoggerFactory
        .getLogger(MethodHandles.lookup().lookupClass());

    public static final double OUR_LOAD_HIGH = 3.0;
    public static final long REFRESH_INTERVAL = TimeUnit.NANOSECONDS.convert(2500, TimeUnit.MILLISECONDS);
    public static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
    private final long refreshIntervalMs;

    private long refreshInterval;
    private  volatile boolean stopped;

    private volatile Map<Long, ThreadTime> threadTimeMap = new HashMap<>(512);
    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private OperatingSystemMXBean opBean = ManagementFactory.getOperatingSystemMXBean();

    private static volatile SysStats sysStats;
    private volatile double totalUsage;
    private volatile double sysLoad;

    public static void main(String[] args) {
        System.out.println("avail procs:" + PROC_COUNT);
    }

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
        this.refreshIntervalMs = TimeUnit.MILLISECONDS.convert(refreshInterval, TimeUnit.NANOSECONDS);
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
        boolean gatherThreadStats = true;
        Collection<ThreadTime> values = null;
        while(!stopped) {
            if (gatherThreadStats) {
                Set<Long> mappedIds = new HashSet<Long>(threadTimeMap.keySet());

                long[] allThreadIds = threadBean.getAllThreadIds();

                removeDeadThreads(mappedIds, allThreadIds);

                mapNewThreads(allThreadIds);
                values = new HashSet<>(threadTimeMap.values());

                for (ThreadTime threadTime : values) {
                    threadTime.setLast(threadBean.getThreadCpuTime(threadTime.getId()));
                }

//                double load =  ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
//                if (load < 0) {
//                    log.warn("SystemLoadAverage not supported on this JVM");
//                } else {
//                    sysLoad = load / (double) PROC_COUNT;
//                }

            } else {
                double load =  ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
                if (load < 0) {
                    log.warn("SystemLoadAverage not supported on this JVM");
                } else {
                    sysLoad = load / (double) PROC_COUNT;
                }
                gatherThreadStats = true;
            }


            try {
                Thread.sleep(refreshIntervalMs);
//                if (stopped) {
//                    return;
//                }
            } catch (InterruptedException e) {
//                ParWork.propegateInterrupt(e, true);
//                return;
            }

            if (gatherThreadStats) {
                double usage = 0D;
                for (ThreadTime threadTime : values) {
                    long time = threadBean.getThreadCpuTime(threadTime.getId());
                    if (time > 0) {
                        long diff = time - threadTime.getLast();
                        if (diff > 0) {
                            usage += diff / (refreshInterval * 2.0f);
                        }
                    }
                }
                totalUsage = usage;
                gatherThreadStats = false;
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

    public double getSystemLoad() {
        double sysLoad = -1;
        double load = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        if (load < 0) {
            log.warn("SystemLoadAverage not supported on this JVM");
        } else {
            sysLoad = load / (double) PROC_COUNT;
        }
        return sysLoad;
    }

    public double getTotalUsage() {
        return totalUsage;
    }

    public double getAvarageUsagePerCPU() {
        return getTotalUsage() / ParWork.PROC_COUNT;
    }

//    public double getUsageByThread(Thread t) {
//        ThreadTime info;
//
//        info = threadTimeMap.get(t.getId());
//
//        double usage = 0D;
//        if(info != null) {
//            synchronized (info) {
//                usage = (info.getCurrent() - info.getLast()) / (TimeUnit.MILLISECONDS.toNanos(refreshInterval * 2));
//            }
//        }
//        return usage;
//    }

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