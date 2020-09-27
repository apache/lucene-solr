package org.apache.solr.servlet;

import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.util.FutureCallback;
import org.eclipse.jetty.util.component.Graceful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Future;

public class SolrShutdownHandler extends HandlerWrapper implements Graceful {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public SolrShutdownHandler() {
        super();
    }

    @Override
    public Future<Void> shutdown() {
        log.error("GRACEFUL SHUTDOWN CALLED");
        return new FutureCallback(true);
    }

    @Override
    public boolean isShutdown() {
        return true;
    }
}
