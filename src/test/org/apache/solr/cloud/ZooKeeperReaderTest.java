package org.apache.solr.cloud;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.zookeeper.KeeperException;

public class ZooKeeperReaderTest extends TestCase {

  private static final String COLLECTION_NAME = "collection1";

  private static final String SHARD2 = "shard2";

  private static final String SHARD1 = "shard1";

  static final String ZOO_KEEPER_HOST = "localhost:2181/solr";

  static final int TIMEOUT = 10000;

  private static final String URL1 = "http://localhost:3133/solr/core0";

  private static final boolean DEBUG = true;

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  public void testReadShards() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZooKeeperTestServer server = new ZooKeeperTestServer(zkDir);
    server.run();

    makeSolrZkNode();

    ZooKeeperWriter writer = new ZooKeeperWriter(ZOO_KEEPER_HOST, TIMEOUT);
    String shardsPath = "/collections/collection1/shards";
    writer.makePath(shardsPath);

    addShardToZk(writer, shardsPath, URL1, SHARD1 + "," + SHARD2);
    addShardToZk(writer, shardsPath, "http://localhost:3123/solr/core1", SHARD1);
    addShardToZk(writer, shardsPath, "http://localhost:3133/solr/core1", SHARD1);

    ZooKeeperReader reader = new ZooKeeperReader(ZOO_KEEPER_HOST, TIMEOUT);

    if (DEBUG) {
      reader.printLayoutToStdOut();
    }

    Map<String,ShardInfoList> shardInfoMap = reader.readShardInfo(shardsPath);
    assertTrue(shardInfoMap.size() > 0);
    Set<Entry<String,ShardInfoList>> entries = shardInfoMap.entrySet();

    if (DEBUG) {
      for (Entry<String,ShardInfoList> entry : entries) {
        System.out.println("shard:" + entry.getKey() + " value:"
            + entry.getValue().toString());
      }
    }

    Set<String> keys = shardInfoMap.keySet();

    assertTrue(keys.size() == 2);

    assertTrue(keys.contains(SHARD1));
    assertTrue(keys.contains(SHARD2));

    ShardInfoList shardInfoList = shardInfoMap.get(SHARD1);

    assertEquals(3, shardInfoList.getShards().size());

    shardInfoList = shardInfoMap.get(SHARD2);

    assertEquals(1, shardInfoList.getShards().size());

    assertEquals(URL1, shardInfoList.getShards().get(0).getUrl());

    server.shutdown();
  }

  public void testReadConfigName() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZooKeeperTestServer server = new ZooKeeperTestServer(zkDir);
    server.run();

    makeSolrZkNode();

    ZooKeeperWriter writer = new ZooKeeperWriter(ZOO_KEEPER_HOST, TIMEOUT);
    String actualConfigName = "firstConfig";
      
    String shardsPath = "/collections/" + COLLECTION_NAME + "/config=" + actualConfigName;
    writer.makePath(shardsPath);
    
    ZooKeeperReader reader = new ZooKeeperReader(ZOO_KEEPER_HOST, TIMEOUT);

    if (DEBUG) {
      reader.printLayoutToStdOut();
    }
    
    String configName = reader.readConfigName(COLLECTION_NAME);
    assertEquals(configName, actualConfigName);
    
  }

  private void addShardToZk(ZooKeeperWriter writer, String shardsPath,
      String url, String shardList) throws IOException, KeeperException,
      InterruptedException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // nocommit: could do xml
    Properties props = new Properties();
    props.put(ZooKeeperController.URL_PROP, url);
    props.put(ZooKeeperController.SHARD_LIST_PROP, shardList);
    props.store(baos, ZooKeeperController.PROPS_DESC);

    writer.makeEphemeralSeqPath(shardsPath
        + ZooKeeperController.NODE_ZKPREFIX, baos.toByteArray(), null);
  }

  private void makeSolrZkNode() throws Exception {
    ZooKeeperWriter zkWriter = new ZooKeeperWriter(ZOO_KEEPER_HOST.substring(0,
        ZOO_KEEPER_HOST.indexOf('/')), TIMEOUT);

    zkWriter.makePath("/solr");
    zkWriter.close();
  }
}
