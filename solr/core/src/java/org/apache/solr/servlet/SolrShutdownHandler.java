package org.apache.solr.servlet;

import org.eclipse.jetty.server.handler.ShutdownHandler;

public class SolrShutdownHandler extends ShutdownHandler {
    public SolrShutdownHandler() {
        super("solrrocks");
    }
}
