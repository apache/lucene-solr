package org.apache.solr.servlet;


import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.solr.rest.API;
import org.apache.solr.rest.APIModule;
import org.apache.solr.rest.ResourceFinder;
import org.restlet.Component;
import org.restlet.data.Protocol;
import org.restlet.ext.servlet.ServerServlet;
import org.restlet.routing.VirtualHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;

/**
 *
 *
 **/
@Singleton
public class InjectableServlet extends ServerServlet {
  private transient static Logger log = LoggerFactory.getLogger(InjectableServlet.class);
  @Inject
  Injector injector;
  private Set<APIModule> modules;

  @Inject
  InjectableServlet(Set<APIModule> modules) {
    this.modules = modules;
    //we have everything we need here

  }


  @Override
  protected Component createComponent() {
    ResourceFinder finder = new ResourceFinder(injector);
    injector.injectMembers(finder);
    Component component = new Component();
    VirtualHost apivhost = new VirtualHost(component.getContext());
    component.getDefaultHost().attach("/api", apivhost);
    for (APIModule module : modules) {

      module.initInjectorDependent();
      API api = injector.getInstance(module.getAPIClass());
      String apiRoot = api.getAPIRoot();
      if (apiRoot == null || apiRoot.equals("")) {
        throw new RuntimeException("API must provide non-null, non-empty getAPIRoot implementation");
      }
      //TODO: set up a proper status service
      //api.setStatusService(statusService);
      apivhost.attach(apiRoot, api);
      log.info("Attached " + api.getAPIName() + " to " + apiRoot);
    }
    // Define the list of supported client protocols.
    component.getClients().add(Protocol.HTTP);
    return component;
  }

  @Override
  public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    //TODO: stats here?
    super.service(request, response);

  }
}
