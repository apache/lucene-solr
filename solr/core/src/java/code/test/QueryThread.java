package code.test;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

public class QueryThread implements Runnable {
  final CloudSolrClient client;

  QueryThread(CloudSolrClient client) {
    this.client = client;
  }

  @Override
  public void run() {
    while (AddDvStress.stopRun.get() == false && AddDvStress.endCycle.get() == false) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "*:*");
      params.add("facet", "true");
      params.add("facet.field", AddDvStress.FIELD);
      params.add("facet.limit", "-1");
      int total = 0;
      long numFound = 0;
      try {
        Thread.sleep(AddDvStress.querySleepInterval.get());
        QueryResponse resp = client.query(params);
        FacetField ff = resp.getFacetField(AddDvStress.FIELD);

        for (FacetField.Count count : ff.getValues()) {
          total += count.getCount();
        }
        numFound = resp.getResults().getNumFound();
        if (numFound != total) {
          System.out.println("Found: " + total + " expected: " + numFound);
          AddDvStress.badState.set(true);
          AddDvStress.querySleepInterval.set(1000);
        } else {
          AddDvStress.badState.set(false);
          AddDvStress.querySleepInterval.set(100);
        }
      } catch (IOException | SolrServerException | InterruptedException e) {
        e.printStackTrace();
        AddDvStress.stopRun.set(true);
      }
      if (AddDvStress.endCycle.get()) {
        System.out.println("Final query, found: " + total + " expected: " + numFound + " docCount: " + AddDvStress.docCount.get());
        if (total != numFound) {
          AddDvStress.stopRun.set(true);
        }
      }
    }
  }
}
