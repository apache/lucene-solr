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

import { ListCollections } from './collections';

@Injectable()
export class CollectionsService {

  private collectionsUrl = '/solr/admin/collections';

  constructor(private http: HttpClient) {
  }

  listCollections(): Observable<String[]> {
    const params: HttpParams = new HttpParams().set('action', 'LIST').set("wt", "json");
    return this.http.get<ListCollections>(this.collectionsUrl, { params: params }).pipe(map(lc => lc.collections));
  }

  collectionInfo(collectionName: string): Observable<Collection> {
    const params: HttpParams = new HttpParams().set('action', 'CLUSTERSTATUS').set("wt", "json");
    return this.http.get<HttpResponse<any>>(this.collectionsUrl, { observe: 'response', params: params }).pipe(map(cs => {
      let c = new Collection();
      c.name = collectionName;
      const body: any = cs.body;
      const cluster: any = body.cluster;
      const collectionInfo = cluster.collections[collectionName];
      const shards = collectionInfo.shards;
      let shardList : Shard[] = [];
      let numShards = 0;
      for(let shard in shards) {
        numShards++;
        let s: Shard = new Shard();
        let sObj = shards[shard];
        s.name = shard;
        s.collectionName = collectionName;
        s.show = false;
        s.showRemove = false;
        s.showAdd = false;
        s.range = sObj.range;
        s.state = sObj.state;
        const replicas = sObj.replicas;
        let replicaList: Replica[] = [];
        for(let replica in replicas) {
          let r: Replica = new Replica();
          let rObj = replicas[replica];
          r.shard = s;
          r.name = replica;
          r.show = false;
          r.showDelete  = false;
          r.core = rObj.core;
          r.base_url = rObj.base_url;
          r.node_name = rObj.node_name;
          r.state = rObj.state;
          r.leader = rObj.leader;
          r.type = rObj.type;
          replicaList.push(r);
        }
        s.replicas = replicaList;
        shardList.push(s);
      }
      c.shardList = shardList;
      c.numShards = numShards;
      c.configName = collectionInfo.configName;
      c.replicationFactor = collectionInfo.replicationFactor;
      c.maxShardsPerNode = collectionInfo.maxShardsPerNode;
      c.routerName = collectionInfo.router.name;
      c.routerField = collectionInfo.router.field;
      c.autoAddReplicas = collectionInfo.autoAddReplicas;
      return c;
    }));
  }

  addCollection(coll: Collection): Observable<any> {
    const params: HttpParams = new HttpParams()
      .set('action', 'CREATE')
      .set("wt", "json")
      .set('name', coll.name ? coll.name : '')
      .set('router.name', coll.routerName ? coll.routerName : '')
      .set('numShards', coll.numShards ? coll.numShards.toString() : '1')
      .set('collection.configName', coll.configName ? coll.configName : '')
      .set('replicationFactor', coll.replicationFactor ? coll.replicationFactor.toString() : '1')
      .set('maxShardsPerNode', coll.maxShardsPerNode ? coll.maxShardsPerNode.toString() : '1')
      .set('autoAddReplicas', coll.autoAddReplicas ? coll.autoAddReplicas.toString() : 'false')
      .set('shards', coll.shards ? coll.shards : '')
      .set('router.field', coll.routerField ? coll.routerField : '');
    return this.http.post<any>(this.collectionsUrl, coll, { params: params });
  }

  addReplica(collectionName: string, shardName: string, replicaNodeName: string): Observable<any> {
    let params: HttpParams = new HttpParams()
      .set('action', 'ADDREPLICA')
      .set("wt", "json")
      .set('collection', collectionName)
      .set('shard', shardName);
    if(replicaNodeName) {
      params = params.set('node', replicaNodeName);
    }
    return this.http.post<any>(this.collectionsUrl, null, { params: params });
  }

  createAlias(aliasName: string, collectionNames: string[]): Observable<any> {
    const params: HttpParams = new HttpParams()
      .set('action', 'CREATEALIAS')
      .set("wt", "json")
      .set('name', aliasName)
      .set('collections', collectionNames.length==0 ? null : collectionNames.join());
    return this.http.post<any>(this.collectionsUrl, null, { params: params });
  }

  deleteAlias(aliasName: string):  Observable<any> {
    const params: HttpParams = new HttpParams()
      .set('action', 'DELETEALIAS')
      .set("wt", "json")
      .set('name', aliasName);
    return this.http.post<any>(this.collectionsUrl, null, { params: params });
  }

  reloadCollection(name: string): Observable<any> {
    const params: HttpParams = new HttpParams()
      .set('action', 'RELOAD')
      .set("wt", "json")
      .set('name', name);
    return this.http.post<any>(this.collectionsUrl, null, { params: params });
  }

  deleteCollection(name: string): Observable<any> {
    const params: HttpParams = new HttpParams()
      .set('action', 'DELETE')
      .set("wt", "json")
      .set('name', name);
    return this.http.post<any>(this.collectionsUrl, null, { params: params });
  }

  deleteReplica(replica: Replica): Observable<any> {
    const params: HttpParams = new HttpParams()
      .set('action', 'DELETEREPLICA')
      .set("wt", "json")
      .set('collection', replica.shard.collectionName)
      .set('replica', replica.name)
      .set('shard', replica.shard.name);
    return this.http.post<any>(this.collectionsUrl, null, { params: params });
  }
}
export class Collection {
  name: string;
  configName: string;
  numShards: number;
  replicationFactor: number;
  routerField: string;
  maxShardsPerNode: number;
  shards: string;
  routerName: string;
  autoAddReplicas: boolean;
  shardList: Shard[];
}

export class Shard {
  name: string;
  collectionName: string;
  show: boolean;
  range: string;
  state: string;
  showAdd: boolean;
  showRemove: boolean;
  replicas: Replica[];
}

export class Replica {
  shard : Shard;
  name: string;
  show: boolean;
  showDelete : boolean;
  core: string;
  base_url: string;
  node_name: string;
  state: string;
  leader: boolean;
  type: String;
}
