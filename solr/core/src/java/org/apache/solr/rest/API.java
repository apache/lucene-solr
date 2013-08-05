package org.apache.solr.rest;


import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.restlet.Application;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Redirector;
import org.restlet.routing.Route;
import org.restlet.routing.Router;
import org.restlet.security.Authenticator;
import org.restlet.security.Role;
import org.restlet.security.RoleAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *
 **/
public abstract class API extends Application implements APIMBean {
  private transient static Logger log = LoggerFactory.getLogger(API.class);
  protected Router router;
  protected ResourceFinder finder;
  protected Collection<String> paths;

  //stats
  volatile long numRequests;
  volatile long numErrors;
  volatile long totalTime = 0;
  long handlerStart = System.currentTimeMillis();

  protected API(ResourceFinder finder) {
    this.finder = finder;
    paths = new ArrayList<String>();
  }

  @Override
  public Restlet createInboundRoot() {
    router = new Router(getContext());
    Restlet guard = guardPossibly();
    initAttachments();
    return guard;
  }

  /**
   * API's that wish to put guards on the Router should override this method with
   * the appropriate wrapping of the Router.  The default is to not
   * guard.
   *
   * @return The Restlet, either the Router or the Guarded Router.
   */
  protected Restlet guardPossibly() {
    return router;
  }

  //Override so we can instrument it

  /**
   * Derived classes can override this with per API statistics
   */
  @Override
  public void handle(Request request, Response response) {
    logStart(request, response);
    long start = System.currentTimeMillis();
    super.handle(request, response);
    long finish = System.currentTimeMillis();
    if (response.getStatus().isError()) {
      numErrors++;
    }
    long diff = finish - start;
    if (log.isInfoEnabled() && request.isLoggable()) {
      log.info(getAPIName() + "/" + getAPIRoot() + " took " + diff + " ms");
    }
    totalTime += diff;
    logFinish(request, response);
    numRequests++;
  }

  /**
   * Provides an easy way for overriding classes to log details about the request/response that are pertinent to that API instead
   * of logging everything in the request.  Is called after all things have finished in the {@link #handle(org.restlet.Request, org.restlet.Response)} method.
   * <p/>
   * By default, logs number of requests, request.toString and response.toString.
   *
   * @param request  The {@link org.restlet.Request}
   * @param response The {@link org.restlet.Response}
   */
  protected void logFinish(Request request, Response response) {
    if (log.isInfoEnabled() && request.isLoggable()) {
      log.info("Finish Req #: " + numRequests + " Req: " + request.toString() + " Rsp: " + response.toString());
    }
  }

  /**
   * Provides an easy way for overriding classes to log details about the request/response that are pertinent to that API instead
   * of logging everything in the request.  Is called after all things have finished in the {@link #handle(org.restlet.Request, org.restlet.Response)} method.
   * <p/>
   * By default, the number of requests and request.toString() are logged at the info level.
   *
   * @param request  The {@link org.restlet.Request}
   * @param response The {@link org.restlet.Response}
   */
  protected void logStart(Request request, Response response) {
    if (log.isInfoEnabled() && request.isLoggable()) {
      log.info("Start Req #: " + numRequests + " Req: " + request.toString());
    }
  }

  protected Route attach(String path, Class<? extends ServerResource> resource) {
    return attach(path, router, resource, (Role[]) null);
  }

  protected Route attach(String path, Router base, Class<? extends ServerResource> resource) {
    return attach(path, base, resource, (Role[]) null);
  }

  protected Route attach(String path, Router base, Class<? extends ServerResource> resource, Role... roles) {
    return attach(path, base, resource, roles != null && roles.length > 0 ? Arrays.asList(roles) : null);
  }

  protected Route attach(String path, Router base, Class<? extends ServerResource> resource, List<Role> roles) {
    Restlet attachment = null;
    if (roles != null && roles.isEmpty() == false) {
      RoleAuthorizer ra = new RoleAuthorizer();
      ra.getAuthorizedRoles().addAll(roles);
      ra.setNext(finder.finderOf(resource));
      attachment = ra;
    } else {
      attachment = finder.finderOf(resource);
    }
    return attach(path, base, attachment, (Authenticator) null);
  }

  protected Route attach(String path, Router base, Class<? extends ServerResource> resource, Authenticator authenticator) {
    return attach(path, base, finder.finderOf(resource), authenticator);
  }

  protected Route attach(String path, Router base, Restlet restlet) {
    return attach(path, base, restlet, (Authenticator) null);
  }

  protected Route attach(String path, Router base, Restlet restlet, Authenticator authenticator) {
    return attach(path, base, restlet, authenticator, Router.MODE_BEST_MATCH);
  }

  protected Route attach(String path, Router base, Restlet restlet, Authenticator authenticator, int mode) {
    Route result;
    if (authenticator != null) {
      authenticator.setNext(restlet);
      result = base.attach(path, authenticator, mode);
    } else {
      result = base.attach(path, restlet, mode);
    }
    if (path.equals("")) {
      path = "/";
    }
    paths.add(path);
    return result;
  }

  protected void redir(URI uri,
                       Router base,
                       String attach,
                       String path, Role... roles) {
    Restlet attachment;
    Redirector redir = new Redirector(router.getContext(),
        uri.toString() + path,
        Redirector.MODE_SERVER_OUTBOUND);
    if (roles != null && roles.length > 0) {
      attachment = getRoleAuthorizer(redir, roles);
    } else {
      attachment = redir;
    }
    attach(attach, base, attachment);
  }


  protected RoleAuthorizer getRoleAuthorizer(Restlet next, Role... roles) {
    RoleAuthorizer ra = new RoleAuthorizer();
    ra.getAuthorizedRoles().addAll(Arrays.asList(roles));
    ra.setNext(next);
    return ra;
  }

  /**
   * Overriding classes implement this method in order to attach resources (ServerResource) to specific endpoints.
   */
  protected abstract void initAttachments();

  /**
   * The root to attach this API to.  Must not be the empty string.
   *
   * @return
   */
  public abstract String getAPIRoot();

  public abstract String getAPIName();

  @Override
  public void stop() throws Exception {
    if (router != null) {
      router.stop();
    }
  }

  @Override
  public Collection<String> getEndpoints() {
    return paths;
  }

  @Override
  public NamedList getStatistics() {

    NamedList namedList = new NamedList();
    namedList.add("numRequests", numRequests);
    namedList.add("numErrors", numErrors);
    namedList.add("totalTime", totalTime);
    namedList.add("startTime", handlerStart);
    return namedList;
  }


  //TODO:  Can we automate all of these so people don't need to to it themselves?
  @Override
  public String getVersion() {
    String result = null;
    return result;
  }

  @Override
  public Category getCategory() {
    Category result = null;
    return result;
  }

  @Override
  public String getSource() {
    String result = null;
    return result;
  }

  @Override
  public URL[] getDocs() {
    URL[] result = new URL[0];
    return result;
  }

  @Override
  public String getName() {
    return getAPIName();
  }
}