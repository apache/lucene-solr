import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { SolrService } from '../solr.service';
import { CloudSystemResponse } from 'src/app/domain/solr-admin-info-system/cloud-system-response';
import { SolrSystemResponse } from '../../domain/solr-admin-info-system/solr-system-response';

@Injectable({
  providedIn: 'root'
})
export class CloudGraphService extends SolrService {

    path = '/solr/admin/collections?_=1586675588233&action=CLUSTERSTATUS&wt=json';
    zkPath = '/solr/admin/zookeeper/status?_=1586767809848&wt=json';
    cloudStatusPath ='/solr/admin/collections?_=1586910250283&action=LIST&wt=json';

  constructor(private http: HttpClient) {
      super();
  }

  get() {
      return this.http.get<any>(this.path);
  }
  getZkStatus(){
    return this.http.get<any>(this.zkPath);
  }
  getCloudStatus(){
    return this.http.get<any>(this.cloudStatusPath);
  }
}