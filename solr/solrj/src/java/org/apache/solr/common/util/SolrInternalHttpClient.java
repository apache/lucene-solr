package org.apache.solr.common.util;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class SolrInternalHttpClient extends HttpClient {

  public SolrInternalHttpClient(HttpClientTransport transport, SslContextFactory sslContextFactory) {
    super(transport, sslContextFactory);
    assert ObjectReleaseTracker.track(this);
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    assert ObjectReleaseTracker.release(this);
  }

}
