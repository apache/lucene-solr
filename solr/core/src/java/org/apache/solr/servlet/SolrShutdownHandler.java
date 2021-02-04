package org.apache.solr.servlet;

import org.apache.solr.common.ParWork;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FutureCallback;
import org.eclipse.jetty.util.component.Graceful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SolrShutdownHandler extends HandlerWrapper implements Graceful {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private final static Set<Runnable> shutdowns = new LinkedHashSet<>();

    public SolrShutdownHandler() {
        super();
    }

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
    public CompletableFuture<Void> shutdown() {
        log.error("SHUTDOWN MONITOR HOOK CALLED");
        return new Callback.Completable();
        //return new VoidShutdownFuture();
    }

    @Override
    public boolean isShutdown() {
        return true;
    }

    private static class VoidShutdownFuture implements Future<Void> {
        @Override
        public boolean cancel(boolean b) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public synchronized boolean isDone() {
            return false;
        }

        @Override
        public synchronized Void get() throws InterruptedException, ExecutionException {
            synchronized (SolrShutdownHandler.class) {
                try (ParWork work = new ParWork(this, true, false)) {
                    for (Runnable run : shutdowns) {
                        work.collect("shutdown", () -> run.run());
                    }
                }
                shutdowns.clear();
            }
            return null;
        }

        @Override
        public synchronized Void get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
            synchronized (SolrShutdownHandler.class) {
                try (ParWork work = new ParWork(this, true, false)) {
                    for (Runnable run : shutdowns) {
                        work.collect("shutdown", () -> run.run());
                    }
                }
                shutdowns.clear();
            }
            return null;
        }
    }
}
