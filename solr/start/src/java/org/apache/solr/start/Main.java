package org.apache.solr.start;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.AsyncNCSARequestLog;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NegotiatingServerConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlets.PushCacheFilter;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class Main {

    private static final Logger LOG = Log.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        try (JettyStarter starter = new JettyStarter();) {
            starter.start();
            LOG.info("Press any key to stop Jetty.");
            System.in.read();
        }
    }

    public static class NoopServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            resp.setContentType("text/html;charset=UTF-8");
            resp.getWriter().append("OK+");
        }

    }

    private static class JettyStarter implements AutoCloseable {

        private static final int SSL_PORT = 8443;
        private final Server server;
        private final HttpConfiguration config = createHttpConfiguration();

        public JettyStarter() {
            HttpConnectionFactory httpFactory = new HttpConnectionFactory(config);
            HTTP2ServerConnectionFactory http2Factory = new HTTP2ServerConnectionFactory(
                    config);
            ALPNServerConnectionFactory alpn = createAlpnProtocolFactory(httpFactory);
            Server server = createServer(httpFactory, http2Factory, alpn);

            HandlerWrapper servletHandler = createServletHandlerWithServlet();
            HandlerWrapper gzipHandler = createGzipHandler();
            gzipHandler.setHandler(servletHandler);
            server.setHandler(gzipHandler);

            this.server = server;
        }

        private Server createServer(HttpConnectionFactory httpConnectionFactory,
                HTTP2ServerConnectionFactory http2ConnectionFactory,
                ALPNServerConnectionFactory alpn) {
            Server server = new Server();
            server.setRequestLog(new AsyncNCSARequestLog());

            ServerConnector connector = new ServerConnector(server, prepareSsl(alpn),
                    alpn, http2ConnectionFactory, httpConnectionFactory);
            connector.setPort(SSL_PORT);
            server.addConnector(connector);

            return server;
        }

        private GzipHandler createGzipHandler() {
            GzipHandler gzipHandler = new GzipHandler();
            gzipHandler.setIncludedPaths("/*");
            gzipHandler.setMinGzipSize(0);
            gzipHandler.setIncludedMimeTypes("text/plain", "text/html");
            return gzipHandler;
        }

        private ServletContextHandler createServletHandlerWithServlet() {
            ServletContextHandler context = new ServletContextHandler(
                    ServletContextHandler.SESSIONS);

            FilterHolder pushCacheFilter = context.addFilter(PushCacheFilter.class, "/*",
                    null);
            Map<String, String> config = new HashMap<>();
            config.put("maxAssociations", "32");
            config.put("ports", Objects.toString(SSL_PORT));
            pushCacheFilter.setInitParameters(config);

            context.addServlet(NoopServlet.class, "/*");
            context.setContextPath("/");

            return context;
        }

        private ALPNServerConnectionFactory createAlpnProtocolFactory(
                HttpConnectionFactory httpConnectionFactory) {
            NegotiatingServerConnectionFactory.checkProtocolNegotiationAvailable();
            ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
            alpn.setDefaultProtocol(httpConnectionFactory.getProtocol());
            return alpn;
        }

        private SslConnectionFactory prepareSsl(ALPNServerConnectionFactory alpn) {
            SslContextFactory sslContextFactory = new SslContextFactory();
            sslContextFactory.setKeyStorePath(
                    Main.class.getResource("/keystore").toExternalForm());
            sslContextFactory.setKeyStorePassword("changeit");
            sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR);
            sslContextFactory.setUseCipherSuitesOrder(true);
            SslConnectionFactory ssl = new SslConnectionFactory(sslContextFactory,
                    alpn.getProtocol());
            return ssl;
        }

        private static HttpConfiguration createHttpConfiguration() {
            HttpConfiguration config = new HttpConfiguration();
            config.setSecureScheme("https");
            config.setSecurePort(SSL_PORT);
            config.setSendXPoweredBy(false);
            config.setSendServerVersion(false);
            config.addCustomizer(new SecureRequestCustomizer());
            return config;
        }

        public void start() throws Exception {
            server.start();
        }

        @Override
        public void close() throws Exception {
            server.stop();
        }

    }

}