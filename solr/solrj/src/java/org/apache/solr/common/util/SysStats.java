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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SysStats extends Thread {
    static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

    private long refreshInterval;
    private boolean stopped;

    private Map<Long, ThreadTime> threadTimeMap = new HashMap<Long, ThreadTime>(512);
    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private OperatingSystemMXBean opBean = ManagementFactory.getOperatingSystemMXBean();

    private static SysStats sysStats;

    public static synchronized SysStats getSysStats() {
        if (sysStats == null) {
            sysStats = new SysStats(10000);
        }
        return  sysStats;
    }

    public SysStats(long refreshInterval) {
        this.refreshInterval = refreshInterval;
        setName("CPUMonitoringThread");
        setDaemon(true);
        start();
    }

    public void doStop() {
        this.stopped = true;
    }

    @Override
    public void run() {
        while(!stopped) {
            Set<Long> mappedIds;
            synchronized (threadTimeMap) {
                mappedIds = new HashSet<Long>(threadTimeMap.keySet());
            }

            long[] allThreadIds = threadBean.getAllThreadIds();

            removeDeadThreads(mappedIds, allThreadIds);

            mapNewThreads(allThreadIds);

            Collection<ThreadTime> values;
            synchronized (threadTimeMap) {
                values = new HashSet<ThreadTime>(threadTimeMap.values());
            }

            for (ThreadTime threadTime : values) {
                synchronized (threadTime) {
                    threadTime.setCurrent(threadBean.getThreadCpuTime(threadTime.getId()));
                }
            }

            try {
                Thread.sleep(refreshInterval);
            } catch (InterruptedException e) {
                ParWork.propegateInterrupt(e);
                throw new RuntimeException(e);
            }

            for (ThreadTime threadTime : values) {
                synchronized (threadTime) {
                    threadTime.setLast(threadTime.getCurrent());
                }
            }

        }
    }

    private void mapNewThreads(long[] allThreadIds) {
        for (long id : allThreadIds) {
            synchronized (threadTimeMap) {
                if(!threadTimeMap.containsKey(id))
                    threadTimeMap.put(id, new ThreadTime(id));
            }
        }
    }

    private void removeDeadThreads(Set<Long> mappedIds, long[] allThreadIds) {
        outer: for (long id1 : mappedIds) {
            for (long id2 : allThreadIds) {
                if(id1 == id2)
                    continue outer;
            }
            synchronized (threadTimeMap) {
                threadTimeMap.remove(id1);
            }
        }
    }

    public void stopMonitor() {
        this.stopped = true;
    }

    public double getTotalUsage() {
        Collection<ThreadTime> values;
        synchronized (threadTimeMap) {
            values = new HashSet<ThreadTime>(threadTimeMap.values());
        }

        double usage = 0D;
        for (ThreadTime threadTime : values) {
            synchronized (threadTime) {
                usage += (threadTime.getCurrent() - threadTime.getLast()) / (refreshInterval * 10000);
            }
        }
        return usage;
    }

    public double getAvarageUsagePerCPU() {
        return getTotalUsage() / opBean.getAvailableProcessors();
    }

    public double getUsageByThread(Thread t) {
        ThreadTime info;
        synchronized (threadTimeMap) {
            info = threadTimeMap.get(t.getId());
        }

        double usage = 0D;
        if(info != null) {
            synchronized (info) {
                usage = (info.getCurrent() - info.getLast()) / (refreshInterval * 10000);
            }
        }
        return usage;
    }

    static class ThreadTime {

        private long id;
        private long last;
        private long current;

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