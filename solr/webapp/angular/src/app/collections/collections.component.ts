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

import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { SystemInfoService } from '../solr.service';
import { SharedService, InitFailure } from '../shared.service';

@Component( {
    selector: 'app-collections',
    templateUrl: './collections.component.html'
} )
export class CollectionsComponent implements OnInit {
    collection: Collection = null;
    newCollection: Collection = null;
    showAdd = false;
    showDelete = false;
    showAdvanced = false;
    showCreateAlias = false;
    showDeleteAlias = false;
    reloadSuccess = true;
    reloadFailure = false;
    addMessage = null;
    swapMessage = null;
    createReplicaMessage = null;
    collectionDeleteConfirm = null;
    aliasToCreate = null;
    aliasToDelete = null;
    
    rootUrl = "ROOT-URL";
    
    configs = ["test config 1" , "test config 2"];
    collections = ["test coll 1", "test coll 2"];
    aliases = ["test alias 1", "test alias 2"];
    nodes = ["test node 1", "test node 2"];

    constructor( private route: ActivatedRoute, private sharedService: SharedService ) { }
    
    refresh() {
        //this.sharedService.exceptions = ['test exception', 'another one'];
        //this.sharedService.showInitFailures = true;
        //this.sharedService.initFailures = [new InitFailure( 'sample core', 'sample error' )];
    }
    
    ngOnInit() {
        this.route.params.subscribe(params => {
            const collectionName = params['name'];
            if(collectionName!==undefined) {
                alert("name: " + collectionName);
            } 
         });        
    }
    
    showAddCollection() {
        this.newCollection = new Collection();
        this.showAdd = true;
    }
    
    cancelAddCollection() {
        this.newCollection = null;
        this.showAdd = false;
    }
    
    addCollection() {
        alert("add: " + this.newCollection.name);
        this.showAdd = false;
    }
    
    reloadCollection() {
        this.reloadSuccess = false;
        this.reloadFailure = true;
        alert("reload collections");
    }
    
    toggleCreateAlias() {
        this.showCreateAlias = !this.showCreateAlias;
    }
    
    toggleDeleteAlias() {
        this.showDeleteAlias = !this.showDeleteAlias;
    }
    
    createAlias() {
        alert("create alias: " + this.aliasToCreate);
        this.aliasToCreate = null;
        this.showCreateAlias = false;
    }
    
    cancelCreateAlias() {
        this.aliasToCreate = null;
        this.showCreateAlias = false;
    }
    
    deleteAlias() {
        alert("delete alias: " + this.aliasToDelete);
        this.aliasToCreate = null;
        this.showDeleteAlias = false;
    }
    
    cancelDeleteAlias() {
       this.aliasToDelete = null;
       this.showDeleteAlias = false;
    }
    
    showDeleteCollection() {
        this.showDelete = true;
    }
    
    hideDeleteCollection() {
        this.showDelete = false;
    }
    
    deleteCollection() {
        alert("delete collection: " + this.collectionDeleteConfirm);
        this.collectionDeleteConfirm = null;        
    }
    
    toggleRemoveShard(shard: Shard) {
        shard.showRemove = !shard.showRemove;
    }
    
    toggleAddReplica(shard: Shard) {
        shard.addReplica = !shard.addReplica;
    }
    
    addReplica(shard: Shard) {
        alert("add replica: " + shard.name);
    }
    
    deleteShard(shard: Shard) {
        
    }
}

export class Collection {
    name: string;
    configName: string;
    numShards: number;
    replicationFactor: number;
    router: string;
    maxShardsPerNode: string;
    shards: string;
    routerName: string;
    autoAddReplicas: boolean;
    shards: Shard[];
}

export class Shard {
    name: string;
    show: boolean;
    range: string;
    state: string;    
    showRemove: boolean;
    replicas: Replica[];
    addReplica: boolean;
    replicaNodeName: String;
}

export class Replica {
    name: string;
    show: boolean;
    core: string;
    base_url: string;
    node_name: string;
    state: string;
    leader: boolean;
    deleted: boolean;
}
