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
export class LukeService {

  constructor(private http: HttpClient) {
  }

  schema(collectionName: String): Observable<any[]> {
    const params: HttpParams = new HttpParams().set('show', 'schema').set('path', '/configs/').set("wt", "json");
    return this.http.get<HttpResponse<any>>(("/solr/" + collectionName + "/admin/luke"), { observe: 'response', params: params })
      .pipe(map(r => {
        let fieldsAndTypes = [];
        const body: any = r.body;
        const fields = body.schema.fields;
        for (let field in fields) {
          fieldsAndTypes.push({ group: "Fields", label: field, value: "fieldname=" + field });
        }
        for (let type in body.schema.types) {
          fieldsAndTypes.push({ group: "Types", label: type, value: "fieldtype=" + type });
        }
        return fieldsAndTypes;
      }));
  }

}
