import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { SolrService } from '../solr.service';
import { JavaPropsResponse } from '../../domain/solr-java-props/java-props-response';

@Injectable({
  providedIn: 'root'
})
export class JavaPropsService extends SolrService {

    path = '/solr/admin/info/properties';

  constructor(private http: HttpClient) {
      super();
  }

  getData() {
      return this.http.get<JavaPropsResponse>(this.path);
  }
}
