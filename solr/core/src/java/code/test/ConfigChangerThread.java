package code.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;

public class ConfigChangerThread implements Runnable {
  final CloudSolrClient client;

  final Random rand = new Random();

  ConfigChangerThread(CloudSolrClient client) {
    this.client = client;
  }
  @Override
  public void run() {
    // Change this the first time the indexing thread is between 1/2 done with the first pass and 1/2 done with the second

    AddDvStress.configsChanged.set(false);
    while (AddDvStress.docCount.get() < AddDvStress.trigger.get()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        System.out.println("Failed to change the configuration");
        AddDvStress.stopRun.set(true);
      }
    }
    System.out.println("Changing the config now, doc count : " + AddDvStress.docCount.get());
    try {
//      AddDvStress.pauseIndexing.set(true);
//      while (AddDvStress.stopRun.get() == false && AddDvStress.indexingPaused.get() == false) {
//        Thread.sleep(100);
//      }
      updateCollectionWithAPI();
//      updateCollectionWithConfigset();
//      AddDvStress.pauseIndexing.set(false);
      System.out.println("successfully changed the config, doc count : " + AddDvStress.docCount.get());
    //} catch (InterruptedException | IOException| SolrServerException e) {
    } catch (IOException| SolrServerException e) {
      e.printStackTrace();
      AddDvStress.stopRun.set(true);
    }
    System.out.println("Done modifying configs and MP, leaving thread");
    AddDvStress.configsChanged.set(true);
  }

//  void updateCollectionWithConfigset() throws IOException, SolrServerException {
//    client.getZkStateReader().getConfigManager().uploadConfigDir(Paths.get(AddDvStress.CONFIGSET_PATH_WITH), CONFIGSET_NAME);
//    System.out.println("Reloading collection with a configset " + AddDvStress.CONFIGSET_PATH_WITH);
//    final CollectionAdminRequest.Reload reloadCollectionRequest = new CollectionAdminRequest.Reload()
//        .setCollectionName(AddDvStress.COLLECTION);
//    CollectionAdminResponse resp = reloadCollectionRequest.process(client);
//    if (resp.getStatus() != 0 || resp.isSuccess() == false) {
//      System.out.println("Failed to reload collection");
//      AddDvStress.stopRun.set(true);
//    }
//
//
//  }
  void updateCollectionWithAPI() throws IOException, SolrServerException {
    System.out.println("Changing the schema");
    // First update the MP
    //    curl 'http://localhost:8983/solr/admin/collections?action=clusterprop&name=ext.mergePolicyFactory.collections.test_dv&
    // val=\{"class":"org.apache.solr.index.AddDocValuesMergePolicyFactory"\}’
    System.out.println("Updating the MP");
    HttpClient httpclient = HttpClients.createDefault();
    HttpPost httppost = new HttpPost(AddDvStress.COLL_ADMIN);
    //curl '
    // http://localhost:8983/solr/admin/collections?action=clusterprop
    // name=ext.mergePolicyFactory.collections.test_dv
    // val=\{"class":"org.apache.solr.index.AddDocValuesMergePolicyFactory"\}'

    // Request parameters and other properties.

    List<NameValuePair> params = new ArrayList<NameValuePair>(3);
    params.add(new BasicNameValuePair("action", "clusterprop"));
    params.add(new BasicNameValuePair("name", AddDvStress.NAME_PREFIX + AddDvStress.COLLECTION));
    params.add(new BasicNameValuePair("val", AddDvStress.MP_VAL));
    httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

    //Execute and get the response.
    HttpResponse response = httpclient.execute(httppost);
    if (response.getStatusLine().getStatusCode() != 200) {
      System.out.println("Changing the MP failed");
      AddDvStress.stopRun.set(true);
    }

    // Reload the collection 1
    System.out.println("Reloading collection 1");
    final CollectionAdminRequest.Reload reloadCollectionRequest = CollectionAdminRequest
        .reloadCollection(AddDvStress.COLLECTION);
    CollectionAdminResponse resp = reloadCollectionRequest.process(client);
    if (resp.getStatus() != 0 || resp.isSuccess() == false) {
      System.out.println("Failed to reload the collection");
      AddDvStress.stopRun.set(true);
    }
    try {
      Thread.sleep(5_000);
    } catch (InterruptedException e) {
      System.out.println("Failed to change the configuration");
      AddDvStress.stopRun.set(true);
    }
    // then update the schema
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", AddDvStress.FIELD);
    fieldAttributes.put("type", "string");
    fieldAttributes.put("stored", false);
    fieldAttributes.put("indexed", true);
    fieldAttributes.put("required", false);
    fieldAttributes.put("docValues", true);
    SchemaRequest.ReplaceField replaceFieldRequest = new SchemaRequest.ReplaceField(fieldAttributes);
    SchemaResponse.UpdateResponse replaceFieldResponse = replaceFieldRequest.process(client);
    if (replaceFieldResponse.getStatus() != 0 || replaceFieldResponse.getResponse().get("errors") != null) {
      System.out.println("Could not modify schema");
      AddDvStress.stopRun.set(true);
    }

    // Reload the collection
    System.out.println("Reloading collection 2");
    resp = reloadCollectionRequest.process(client);
    if (resp.getStatus() != 0 || resp.isSuccess() == false) {
      System.out.println("Failed to create collection");
      AddDvStress.stopRun.set(true);
    }
    
    System.out.println("Done modifying configs and MP, leaving thread");
  }
}
