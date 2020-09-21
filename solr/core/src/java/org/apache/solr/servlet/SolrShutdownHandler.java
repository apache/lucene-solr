package org.apache.solr.servlet;

import org.apache.solr.common.ParWork;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ShutdownHandler;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class SolrShutdownHandler extends ShutdownHandler {
    public SolrShutdownHandler() {
        super("solrrocks");
    }

    protected void doShutdown(Request baseRequest, HttpServletResponse response) throws IOException {
        for (Connector connector : getServer().getConnectors()) {
            connector.shutdown();
        }

        baseRequest.setHandled(true);
        response.setStatus(200);
        response.flushBuffer();

        final Server server = getServer();
        new Thread() {
            @Override
            public void run() {
                try {
                    shutdownServer(server);
                } catch (InterruptedException e) {

                } catch (Exception e) {
                    throw new RuntimeException("Shutting down server", e);
                }
            }
        }.start();
    }

    private void shutdownServer(Server server) throws Exception
    {
        server.stop();
        ParWork.shutdownRootSharedExec();
        System.exit(0);
    }

}
