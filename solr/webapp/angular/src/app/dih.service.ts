/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpResponse, HttpParams } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { of } from 'rxjs/observable/of';
import { catchError, map, tap } from 'rxjs/operators';

@Injectable()
export class DihService {

  constructor(private http: HttpClient) {
  }

  data(collectionName: string): Observable<any> {
    const params: HttpParams = new HttpParams().set('stats', 'true').set("wt", "json");
    return this.http.get<HttpResponse<any>>(("/solr/" + collectionName + "/admin/mbeans"), { observe: 'response', params: params })
      .pipe(map(r => {
        const body: any = r.body;
        return body['solr-mbeans'];
      }));
  }
  config(dihPath: string): Observable<string> {
    const params: HttpParams = new HttpParams().set("command", "show-config");
    const uri = "/solr" + (dihPath.startsWith("/") ? "" : "/") + dihPath;
    return this.http.request('get', uri, { observe: 'body', params: params, responseType: 'text' });
  }

  dih(dihPath: string, paramMap: Map<string, string>, usePost: boolean): Observable<DihInfo> {
    let params: HttpParams = new HttpParams().set("wt", "json");
    paramMap.forEach((value: string, key: string) => {
      params = params.set(key, value);
    });
    const uri = "/solr" + (dihPath.startsWith("/") ? "" : "/") + dihPath;
    if (usePost) {
      const header = new HttpHeaders().set("Content-Type", "application/x-www-form-urlencoded");
      return this.http.post<HttpResponse<any>>(uri, params.toString(), { observe: 'response', headers: header })
        .pipe(map(r => { return this.parseInfo(r.body); }));
    } else {
      return this.http.get<HttpResponse<any>>(uri, { observe: 'response', params: params })
        .pipe(map(r => { return this.parseInfo(r.body); }));
    }
  }
  private parseInfo(body: any): DihInfo {
    const info: DihInfo = new DihInfo();
    const sm: any = body['statusMessages'];
    info.status = body['status'];
    info.importResponse = body['importResponse'];
    if (sm) {
      info.totalRequests = sm['Total Requests made to DataSource'];
      info.totalRowsFetched = sm['Total Rows Fetched'];
      info.totalDocumentsProcessed = sm['Total Documents Processed'];
      info.totalDocumentsSkipped = sm['Total Documents Skipped'];
      info.started = this.formatDate(sm['Full Dump Started']);
      info.aborted = this.formatDate(sm['Aborted']);
      info.rolledBack = this.formatDate(sm['Rolledback']);
      info.committed = this.formatDate(sm['Committed']);
      info.timeTaken = this.parseSeconds(sm['Time taken']);
      info.text = sm[''];
    }
    info.rawStatus = JSON.stringify(body, null, 2);
    return info;
  }
  private parseSeconds(time: string): number {
    let seconds = 0;
    if (time) {
      let arr = time.split('.');
      let parts = arr[0].split(':').reverse();
      for (let i = 0; i < parts.length; i++) {
        seconds += (parseInt(parts[i], 10) || 0) * Math.pow(60, i);
      }
      if (arr[1] && 5 <= parseInt(arr[1][0], 10)) {
        seconds++; // treat more or equal than .5 as additional second
      }
    }
    return seconds;
  }

  private formatDate(value: string): string {
    if (value) {
      value = value.replace(" ", "T") + ".000Z";
    }
    return value;
  }
}

export class DihInfo {
  status: string;
  importResponse: string;
  totalRequests: number;
  totalRowsFetched: number;
  totalDocumentsProcessed: number;
  totalDocumentsSkipped: number;
  started: string;
  aborted: string;
  rolledBack: string;
  committed: string;
  timeTaken: number;
  text: string;
  rawStatus: string;
}
