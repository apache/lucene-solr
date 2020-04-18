import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { SolrService } from '../solr.service';
import { LoggingResponse } from 'src/app/domain/solr-logging/logging-response';

@Injectable({
  providedIn: 'root'
})
export class LoggingService extends SolrService {

    path = '/solr/admin/info/logging?_=1586494826687&since=0&wt=json';

  constructor(private http: HttpClient) {
      super();
  }

  getData() {
      return this.http.get<LoggingResponse>(this.path);
  }
}
