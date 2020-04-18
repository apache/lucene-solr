// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
