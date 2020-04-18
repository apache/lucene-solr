import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { SolrService } from '../solr.service';

@Injectable({
  providedIn: 'root'
})
export class SolrCollectionsService extends SolrService {

    path = '/api/c';

  constructor(private http: HttpClient) {
      super();
  }

  get() {
      return this.http.get<any>(this.path);
  }
}
