import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { SolrService } from '../solr.service';
import { ThreadDumpResponse } from '../../domain/solr-thread-dump/thread-dump-response';

@Injectable({
  providedIn: 'root'
})
export class ThreadDumpService extends SolrService {

    path = '/solr/admin/info/threads?_=1586589643248&wt=json';

  constructor(private http: HttpClient) {
      super();
  }

  getData() {
      return this.http.get<ThreadDumpResponse>(this.path);
  }
}