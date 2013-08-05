package org.apache.solr.rest.ping;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.solr.rest.API;
import org.apache.solr.rest.ResourceFinder;


/**
 * A Trivial API for pinging
 */
@Singleton
public class PingAPI extends API {
  @Inject
  public PingAPI(ResourceFinder finder) {
    super(finder);
  }

  @Override
  protected void initAttachments() {
    attach("", PingSR.class);
  }


  @Override
  public String getAPIRoot() {
    return "/ping";
  }

  @Override
  public String getAPIName() {
    return "PING";
  }

}

