package org.apache.solr.core;


import org.apache.solr.rest.APIModule;

/**
 *
 *
 **/
public class CoreContainerAPIModule extends APIModule {

  @Override
  protected void defineBindings() {
    CoreContainer cores = new CoreContainer();
    cores.load();
    bind(CoreContainer.class).toInstance(cores);
    bind(CoreContainerResource.class).to(CoreContainerProxySR.class);
  }
}
