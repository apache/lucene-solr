package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class CloudSolrServer extends SolrServer {
  private volatile ZkController zkController;
  private String zkHost; // the zk server address
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  private String defaultCollection;
  private LBHttpSolrServer lbServer;
  Random rand = new Random();

  /**
   * @param zkHost The address of the zookeeper quorum containing the cloud state
   */
  public CloudSolrServer(String zkHost) throws MalformedURLException {
      this(zkHost, new LBHttpSolrServer());
  }

  /**
   * @param zkHost The address of the zookeeper quorum containing the cloud state
   */
  public CloudSolrServer(String zkHost, LBHttpSolrServer lbServer) {
    this.zkHost = zkHost;
    this.lbServer = lbServer;
  }

  /** Sets the default collection for request */
  public void setDefaultCollection(String collection) {
    this.defaultCollection = collection;
  }

  /** Set the connect timeout to the zookeeper ensemble in ms */
  public void setZkConnectTimeout(int zkConnectTimeout) {
    this.zkConnectTimeout = zkConnectTimeout;
  }

  /** Set the timeout to the zookeeper ensemble in ms */
  public void setZkClientTimeout(int zkClientTimeout) {
    this.zkClientTimeout = zkClientTimeout;
  }

  /**
   * Connect to the zookeeper ensemble.
   * This is an optional method that may be used to force a connect before any other requests are sent.
   *
   * @throws IOException
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public void connect() {
    if (zkController != null) return;
    synchronized(this) {
      if (zkController != null) return;
      try {
        ZkController zk = new ZkController(zkHost, zkConnectTimeout, zkClientTimeout, null, null, null);
        zk.addShardZkNodeWatches();
        zk.updateCloudState(true);
        zkController = zk;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (KeeperException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);

      } catch (IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);

      } catch (TimeoutException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }
    }
  }


  @Override
  public NamedList<Object> request(SolrRequest request) throws SolrServerException, IOException {
    connect();

    CloudState cloudState = zkController.getCloudState();

    String collection = request.getParams().get("collection", defaultCollection);

    // TODO: allow multiple collections to be specified via comma separated list

    Map<String,Slice> slices = cloudState.getSlices(collection);
    Set<String> liveNodes = cloudState.getLiveNodes();

    // IDEA: have versions on various things... like a global cloudState version
    // or shardAddressVersion (which only changes when the shards change)
    // to allow caching.

    // build a map of unique nodes
    // TODO: allow filtering by group, role, etc
    Map<String,ZkNodeProps> nodes = new HashMap<String,ZkNodeProps>();
    List<String> urlList = new ArrayList<String>();
    for (Slice slice : slices.values()) {
      for (ZkNodeProps nodeProps : slice.getShards().values()) {
        String node = nodeProps.get(ZkController.NODE_NAME);
        if (!liveNodes.contains(node)) continue;
        if (nodes.put(node, nodeProps) == null) {
          String url = nodeProps.get(ZkController.URL_PROP);
          urlList.add(url);
        }
      }
    }

    Collections.shuffle(urlList, rand);
    // System.out.println("########################## MAKING REQUEST TO " + urlList);
    // TODO: set distrib=true if we detected more than one shard?
    LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(request, urlList);
    LBHttpSolrServer.Rsp rsp = lbServer.request(req);
    return rsp.getResponse();
  }

  public void close() {
    if (zkController != null) {
      synchronized(this) {
        if (zkController != null)
          zkController.close();
        zkController = null;
      }
    }
  }
}
