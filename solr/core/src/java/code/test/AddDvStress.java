package code.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

public class AddDvStress {

  static AtomicBoolean stopRun = new AtomicBoolean(false);
  static AtomicBoolean endCycle = new AtomicBoolean(false);
  static AtomicBoolean badState = new AtomicBoolean(false);
//  static AtomicBoolean pauseIndexing = new AtomicBoolean(false);
//  static AtomicBoolean indexingPaused = new AtomicBoolean(false);
  static AtomicInteger querySleepInterval = new AtomicInteger(100);
  static AtomicInteger docCount = new AtomicInteger(0);

  //static int DOCS_PER_PASS = 10_000;

  // TODO try pausing indexing while changing the merge policy after not pausing and see if still a problem
  static String COLLECTION = "eoe";
  static String FIELD = "mystring";
  static String CONFIGSET_PATH_WITHOUT = "/Users/ab/tmp/without/conf";
  //static String CONFIGSET_PATH_WITH = "/Users/Erick/with/conf";
  static String CONFIGSET_NAME = "without";
  static String ZK = "localhost:9983";
  static AtomicInteger docsPerPass = new AtomicInteger();
  static AtomicInteger globalId = new AtomicInteger();
  static AtomicInteger batchSize= new AtomicInteger();
  static AtomicInteger trigger= new AtomicInteger();
  static AtomicBoolean configsChanged = new AtomicBoolean(false);


  Random rand =  new Random();
  CloudSolrClient client;

  public static void main(String[] args) {
    try {
      AddDvStress dvs = new AddDvStress();
      dvs.doTests();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void doTests() throws InterruptedException, SolrServerException, KeeperException, IOException {
    try (CloudSolrClient csc = new CloudSolrClient.Builder(Collections.singletonList(ZK), Optional.empty()).build()) {
      client = csc;
      client.connect();
      int cycle = 0;
      while (stopRun.get() == false) {
        System.out.println("\n\n\n\nStarting cycle: " + cycle++);
        COLLECTION = "eoe-" + cycle;
        client.setDefaultCollection(COLLECTION);
        docCount.set(0);
        docsPerPass.set(rand.nextInt(100_000) + 10_000);
        batchSize.set(rand.nextInt(1_000) + 100);
        trigger.set(rand.nextInt(docsPerPass.get()) + AddDvStress.docsPerPass.get() / 2);
        System.out.println("Starting run with docsPerPass =  " + docsPerPass.get() + " batchSize = " + batchSize.get() + " trigger for changing config = " + trigger.get());
        runTest();
      }
    }
  }

  // Things to wonder about
  // Somehow curl to update isn't doing the right thing or there's a race condition here?
  // Can you generate the problem if you upload a full configset?
  void runTest() throws IOException, SolrServerException, InterruptedException, KeeperException {
    Thread indexer = null;
    Thread queryer = null;
    endCycle.set(false);
    teardown(); // Don't teardown() at the end. In case of abnormal termination, we want to see what's there.
    setup();
    if (stopRun.get() == false) {
      Thread changer = new Thread(new ConfigChangerThread(client));
      changer.start();

      indexer = new Thread(new IndexerThread(client));
      indexer.start();

      queryer = new Thread(new QueryThread(client));
      queryer.start();

      // Now sleep for 5 seconds to let the query thread run for a bit more
      System.out.println("Waiting 5 seconds for query thread to have a final go");
      querySleepInterval.set(1000);
      Thread.sleep(5000);

//      //
//      if (badState.get()) {
//        System.out.println("Trying an optimize to 1 segment");
//        client.optimize(COLLECTION, true, true, 1);
//        System.out.println("Optimize done");
//        Thread.sleep(2000); // give the query process time for another go-round.
//      }
      indexer.join();
      endCycle.set(true);
      queryer.join();
      changer.join(); // should be totally immediate.
    }
  }

  static String COLL_ADMIN = "http://localhost:8983/solr/admin/collections";
  static final String NAME_PREFIX = "ext.mergePolicyFactory.collections.";
  static final String MP_VAL = "{\"class\":\"org.apache.solr.index.AddDocValuesMergePolicyFactory\"}";
  static final String CLUSTERPROPPATH = "/clusterprops.json";

  void setup() throws IOException, SolrServerException, InterruptedException {
    System.out.println("Uploding the configset and creating the collection");
    // upload the configset
    client.getZkStateReader().getConfigManager().uploadConfigDir(Paths.get(CONFIGSET_PATH_WITHOUT), CONFIGSET_NAME);

    // create the collection
    CollectionAdminRequest.Create createCollectionRequest = CollectionAdminRequest
        .createCollection(COLLECTION, CONFIGSET_NAME, 1, 1);
    CollectionAdminResponse resp = createCollectionRequest.process(client);
    if (resp.getStatus() != 0 || resp.isSuccess() == false) {
      System.out.println("Failed to create collection");
      stopRun.set(true);
    }

    // Make sure the collection is active
    for (int idx = 0; idx < 25; idx++) {
      try {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", 1);
        doc.addField(AddDvStress.FIELD, "aa");
        client.add(doc);
        client.commit(false, false);
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }
  }

  boolean zkNodeExists(SolrZkClient zkClient, String path) throws InterruptedException {
    boolean OK = false;
    for (int idx = 0; idx < 10 && OK == false; ++idx) {
      try {
        if (zkClient.exists("/collections/" + COLLECTION, true)) {
          Thread.sleep(1000);
        }
        OK = true;
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }
    if (OK == false) {
      System.out.println("Found collection when it shouldn't be there!");
      stopRun.set(true);
    }
    return OK;
  }
  void teardown() throws IOException, SolrServerException, KeeperException, InterruptedException {
    System.out.println("Tearing down anything remaining from prior run");
    // delete the collection
    SolrZkClient zkClient = client.getZkStateReader().getZkClient();
    if (zkClient.exists("/collections/" + COLLECTION, true)) {
      CollectionAdminRequest.Delete deleteCollectionRequest = CollectionAdminRequest
          .deleteCollection(COLLECTION);
      deleteCollectionRequest.process(client);
    }

    if (zkNodeExists(zkClient, "/collections/" + COLLECTION) == false) return;

    // delete the configset
    if (zkClient.exists("/configs/" + CONFIGSET_NAME, true)) {
      removeConfigSet();
    }

    if (zkNodeExists(zkClient, "/configs/" + CONFIGSET_NAME) == false) return;

    // ensure the cluster property is nuked
    if (zkClient.exists(CLUSTERPROPPATH, true)) {
      byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
      zkClient.setData(CLUSTERPROPPATH, bytes, false);
      boolean OK =  false;
      for (int idx = 0; idx < 10 && OK == false; idx++) {
        String props = new String(zkClient.getData(CLUSTERPROPPATH, null, null, false));
        if (props.equals("{}")) {
          OK = true;
        } else {
          Thread.sleep(1000);
        }
      }
      if (OK == false) {
        System.out.println("found cluster props with content");
        stopRun.set(true);
      }
    }
  }

  void removeConfigSet() throws KeeperException, InterruptedException {
    SolrZkClient zkClient = client.getZkStateReader().getZkClient();
    List<String> descendents = new ArrayList<>();
    descendents.add("/configs/" + CONFIGSET_NAME);

    getConfigList(zkClient, descendents, "/configs/" + CONFIGSET_NAME);

    for (int idx = descendents.size() - 1; idx >= 0; idx--) {
      zkClient.delete(descendents.get(idx), -1, true);
    }
  }

  void getConfigList(SolrZkClient zkClient, List<String> descendents, String path) throws KeeperException, InterruptedException {
    List<String> kids = zkClient.getChildren(path, null, true);
    // First add all the kids at this level
    for (String kid : kids) {
      descendents.add(path + "/" + kid);
    }
    // now get their children
    for (String kid : kids) {
      getConfigList(zkClient, descendents, path + "/" + kid);
    }
  }
}
