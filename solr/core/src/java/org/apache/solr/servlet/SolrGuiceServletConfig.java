package org.apache.solr.servlet;


import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainerAPIModule;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.rest.APIModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 *
 *
 **/
public class SolrGuiceServletConfig extends GuiceServletContextListener {
  private transient static Logger log = LoggerFactory.getLogger(GuiceServletContextListener.class);

  // TODO: figure out how to not have this as a static ?
  //private static final LWEModule module = new LWEModule();
  @Inject protected Injector injector;
  protected final Map<SolrConfig, SolrRequestParsers> parsers = new WeakHashMap<SolrConfig, SolrRequestParsers>();


  /*static {

    try {
      injector = Guice.createInjector(module, new LWEServletModule());
      module.init(injector);
    } catch (Throwable t) {
      final String msg = "Error initializing Guice injector" + t.getMessage();
      log.error(msg, t);
      throw new RuntimeException(msg, t);
    }
  }
*/
  public SolrGuiceServletConfig() {

  }

  @Override
  public void contextInitialized(final ServletContextEvent servletContextEvent) {
    final ServletContext context = servletContextEvent.getServletContext();
    final Set<AbstractModule> modules = new HashSet<>();
    final Set<APIModule> apiModules = new HashSet<>();
    //TODO: find all the Modules on the classpath automatically
    final String moduleNames = context.getInitParameter("api-modules");
    if (moduleNames != null) {
      String[] splits = moduleNames.split(",");
      if (splits != null) {
        for (String moduleName : splits) {
          try {
            Class<? extends APIModule> theModule = Class.forName(moduleName).asSubclass(APIModule.class);
            APIModule mod = theModule.newInstance();
            modules.add(mod);
            apiModules.add(mod);
          } catch (Exception e) {
            log.error("Exception", e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to load: " + moduleName, e);
          }
        }
      }
    } else {
      log.warn("No APIModules specified");
    }
    //Always add the CoreContainerAPI
    CoreContainerAPIModule ccam = new CoreContainerAPIModule();
    modules.add(ccam);
    apiModules.add(ccam);

    AbstractModule module = new AbstractModule() {
      @Override
      protected void configure() {
        Multibinder<APIModule> apiBinder = Multibinder.newSetBinder(binder(), APIModule.class);
        for (APIModule apiModule : apiModules) {
          apiBinder.addBinding().toInstance(apiModule);
        }
      }
    };
    modules.add(module);
    ServletModule servletModule = new ServletModule() {
      @Override
      protected void configureServlets() {
        //TODO: get rid of this
        //Legacy
        //TODO: make request scoped and go away from Servlet?
        serve("/zookeeper*").with(ZookeeperInfoServlet.class);
        //serve("/admin*").with();
        //TODO: do we really need this
        //serve("/admin.html").with(LoadAdminUiServlet.class);
        //TODO: inject existing request handlers?  Or just get rid of them?  Or auto-redirect them?

        //Injectable, RESTlet, etc.
        Map<String, String> initParams = new HashMap<>();
        initParams.put("org.restlet.component", "embedded");

        serve("/*").with(InjectableServlet.class, initParams);
      }
    };
    modules.add(new HttpServletModule(parsers));
    modules.add(servletModule);
    injector = Guice.createInjector(modules);
  }


  @Override
  public void contextDestroyed(ServletContextEvent servletContextEvent) {
    CoreContainer cores = injector.getInstance(CoreContainer.class);
    if (cores != null) {
      cores.shutdown();
    }
  }

  @Override
  protected Injector getInjector() {
    return injector;
  }


}