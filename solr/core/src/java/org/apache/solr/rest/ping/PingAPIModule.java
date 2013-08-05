package org.apache.solr.rest.ping;


import org.apache.solr.rest.APIModule;

public class PingAPIModule extends APIModule {

  @Override
  protected void defineBindings() {
    bind(PingResource.class).to(PingSR.class);
  }

}
