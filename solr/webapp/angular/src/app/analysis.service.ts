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
export class AnalysisService {

  constructor(private http: HttpClient) {
  }

  fieldAnalysis(collectionName: string,
    fieldValue: string,
    query: string,
    fieldName: string,
    fieldType: string,
    verbose: boolean): Observable<any> {
    let params: HttpParams = new HttpParams()
      .set("wt", "json")
      .set("analysis.showmatch", "true")
      .set("verbose_output", (verbose ? "1" : "0"));
    if (fieldValue) {
      params = params.set("analysis.fieldvalue", fieldValue);
    }
    if (query) {
      params = params.set("analysis.query", query);
    }
    if (fieldName) {
      params = params.set("analysis.fieldname", fieldName);
    }
    if (fieldType) {
      params = params.set("analysis.fieldtype", fieldType);
    }

    return this.http.get<HttpResponse<any>>(("/solr/" + collectionName + "/analysis/field"), { observe: 'response', params: params })
      .pipe(map(r => {
        const body: any = r.body;
        const analysis = body.analysis;
        let ft = null;
        if (fieldName) {
          for (let name in analysis.field_names) {
            if (name == fieldName) {
              ft = analysis.field_names[name];
              break;
            }
          }
        }
        if (fieldType) {
          for (let type in analysis.field_types) {
            if (type == fieldType) {
              ft = analysis.field_types[type];
              break;
            }
          }
        }
        let response = {};
        if (ft) {
          this.extractComponents(ft.index, response, "index");
          this.extractComponents(ft.query, response, "query");
        }
        return response;
      }));
  }
  extractComponents(data: any, result: any, name: string) {
    if (data) {
      result[name] = [];
      for (let i = 0; i < data.length; i += 2) {
        let component = {
          name: data[i],
          short: this.getShortComponentName(data[i]),
          captions: this.getCaptionsForComponent(data[i + 1]),
          tokens: this.getTokensForComponent(data[i + 1])
        };
        result[name].push(component);
      }
    }
  }
  getShortComponentName(longname: string) {
    let short = -1 !== longname.indexOf('$')
      ? longname.split('$')[1]
      : longname.split('.').pop();
    return short.match(/[A-Z]/g).join('');
  }

  getCaptionsForComponent(data: any) {
    let captions = [];
    for (let key in data[0]) {
      key = key.replace(/.*#/, '');
      if (key != "match" && key != "positionHistory") {
        captions.push(key.replace(/.*#/, ''));
      }
    }
    return captions;
  }

  getTokensForComponent(data: any) {
    let tokens = [];
    let previousPosition = 0;
    let index = 0;
    for (let i in data) {
      let tokenhash = data[i];
      let positionDifference = tokenhash.position - previousPosition;
      for (let j = positionDifference; j > 1; j--) {
        tokens.push({ position: tokenhash.position - j + 1, keys: [], blank: true, index: index++ });
      }

      let token = { position: tokenhash.position, keys: [], blank: false, index: index++ };

      for (let key in tokenhash) {
        if (key == "match" || key == "positionHistory") {
          //skip, to not display these keys in the UI
        } else {
          let tokenInfo = {};
          tokenInfo['name'] = key;
          tokenInfo['value'] = tokenhash[key];
          if ('text' === key || 'raw_bytes' === key) {
            if (tokenhash.match) {
              tokenInfo['extraclass'] = 'match'; //to highlight matching text strings
            }
          }
          token.keys.push(tokenInfo);
        }
      }
      tokens.push(token);
      previousPosition = tokenhash.position;
    }
    return tokens;
  }

}
