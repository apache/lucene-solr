import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { SolrService } from '../solr.service';
import { SolrSystemResponse } from '../../domain/solr-admin-info-system/solr-system-response';
import { CloudSystemResponse } from 'src/app/domain/solr-admin-info-system/cloud-system-response';

@Injectable({
  providedIn: 'root'
})
export class SystemService extends SolrService {

    path = '/solr/admin/info/system';

  constructor(private http: HttpClient) {
      super();
  }

  getData() {
      return this.http.get<SolrSystemResponse>(this.path);
  }
  getCloudData(nodes: string){
    return this.http.get<CloudSystemResponse>(`${this.path}?_=1586714653959&nodes=${nodes}&wt=json`);
  }
}
