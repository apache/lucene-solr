import { Injectable } from '@angular/core';
import { SolrService } from '../solr.service';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class SolrAdminMetricsService extends SolrService {

    path = '/solr/admin/metrics';
  constructor(private http: HttpClient) {
      super();
  }

  getJvmData() {
      return this.http.get<any>(this.path + '?group=jvm');
  }
  getCloudJvmData(nodes: any) {
    return this.http.get<any>(this.path + `?_=1586675588233&nodes=${nodes}&prefix=CONTAINER.fs,org.eclipse.jetty.server.handler.DefaultHandler.get-requests,INDEX.sizeInBytes,SEARCHER.searcher.numDocs,SEARCHER.searcher.deletedDocs,SEARCHER.searcher.warmupTime&wt=json`);
  }
}
