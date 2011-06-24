package org.apache.solr.handler.component;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author noble.paul@teamaol.com (noblep01)
 *         Date: 6/21/11
 *         Time: 12:14 PM
 */
public class HttpShardHandlerFactory extends ShardHandlerFactory implements PluginInfoInitialized{
  protected static Logger log = LoggerFactory.getLogger(HttpShardHandlerFactory.class);

   // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
   Executor commExecutor = new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
          new SynchronousQueue<Runnable>()  // directly hand off tasks
  );


  HttpClient client;
  Random r = new Random();
  LBHttpSolrServer loadbalancer;
  int soTimeout = 0; //current default values
  int connectionTimeout = 0; //current default values
  public  String scheme = "http://"; //current default values
 // socket timeout measured in ms, closes a socket if read
  // takes longer than x ms to complete. throws
  // java.net.SocketTimeoutException: Read timed out exception
  static final String INIT_SO_TIMEOUT = "socketTimeout";

  // connection timeout measures in ms, closes a socket if connection
  // cannot be established within x ms. with a
  // java.net.SocketTimeoutException: Connection timed out
  static final String INIT_CONNECTION_TIMEOUT = "connTimeout";

  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";



  public ShardHandler getShardHandler(){
    return new HttpShardHandler(this);
  }

  public void init(PluginInfo info) {

    if (info.initArgs != null) {
      Object so = info.initArgs.get(INIT_SO_TIMEOUT);
      if (so != null) {
        soTimeout = (Integer) so;
        log.info("Setting socketTimeout to: " + soTimeout);
      }

      Object urlScheme = info.initArgs.get(INIT_URL_SCHEME);
      if (urlScheme != null) {
        scheme = urlScheme + "://";
        log.info("Setting urlScheme to: " + urlScheme);
      }
      Object co = info.initArgs.get(INIT_CONNECTION_TIMEOUT);
        if (co != null) {
          connectionTimeout = (Integer) co;
          log.info("Setting shard-connection-timeout to: " + connectionTimeout);
        }
    }
    MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
    mgr.getParams().setDefaultMaxConnectionsPerHost(20);
    mgr.getParams().setMaxTotalConnections(10000);
    mgr.getParams().setConnectionTimeout(connectionTimeout);
    mgr.getParams().setSoTimeout(soTimeout);
    // mgr.getParams().setStaleCheckingEnabled(false);

    client = new HttpClient(mgr);

    // prevent retries  (note: this didn't work when set on mgr.. needed to be set on client)
    DefaultHttpMethodRetryHandler retryhandler = new DefaultHttpMethodRetryHandler(0, false);
    client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, retryhandler);

    try {
      loadbalancer = new LBHttpSolrServer(client);
    } catch (MalformedURLException e) {
      // should be impossible since we're not passing any URLs here
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,e);
    }

  }
}
