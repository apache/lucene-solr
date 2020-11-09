package org.apache.solr.common.util;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class SolrInternalHttpClient extends HttpClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public SolrInternalHttpClient(HttpClientTransport transport, SslContextFactory sslContextFactory) {
    super(transport, sslContextFactory);
    assert ObjectReleaseTracker.track(this);
  }

  @Override
  protected void doStop() throws Exception {
    log.info("Stopping {}", this.getClass().getSimpleName());
    super.doStop();
    assert ObjectReleaseTracker.release(this);
  }

}
