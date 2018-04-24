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
import { CollectionsService, Collection, Shard, Replica } from '../collections.service';
import { ZookeeperService } from '../zookeeper.service';
import { SharedService, InitFailure } from '../shared.service';

@Component({
  selector: 'app-collections',
  templateUrl: './collections.component.html'
})
export class CollectionsComponent implements OnInit {
  sharedService: SharedService;
  configs: String[];
  newCollection: Collection = null;
  collection: Collection = null;
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

  aliases = ["test alias 1", "test alias 2"];
  nodes = ["test node 1", "test node 2"];

  constructor(private route: ActivatedRoute,
    sharedService: SharedService,
    private collectionsService: CollectionsService,
    private zookeeperService: ZookeeperService) {
      this.sharedService = sharedService;
    }

  refresh() {
    this.sharedService.refreshCollections();
    this.zookeeperService.listConfigsets().subscribe(c => {
      this.configs = c;
    });
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      const collectionName = params['name'];
      if (collectionName !== undefined) {
        this.sharedService.loaded = false;
        this.collectionsService.collectionInfo(collectionName).subscribe(c => {
          this.collection = c;
          this.sharedService.loaded = true;
        });
      }
    });
    this.refresh();
  }

  showAddCollection() {
    this.addMessage = null;
    this.newCollection = new Collection();
    this.newCollection.numShards = 1;
    this.newCollection.replicationFactor = 1;
    this.newCollection.routerName = "compositeId";
    this.newCollection.maxShardsPerNode = 1;
    this.newCollection.autoAddReplicas = false;
    this.showAdd = true;
  }

  cancelAddCollection() {
    this.newCollection = null;
    this.showAdd = false;
  }

  addCollection() {
    this.sharedService.clearErrors();
    if (!this.newCollection.name) {
      this.addMessage = "Please provide a collection name";
    } else {
      this.sharedService.loaded = false;
      this.collectionsService.addCollection(this.newCollection).subscribe(c => {
        this.sharedService.refreshCollections();
        this.newCollection = null;
        this.showAdd = false;
        this.sharedService.loaded = true;
        this.newCollection = null;
        this.showAdd = false;
      }, (error => {
        this.sharedService.showError(error);
        this.sharedService.loaded = true;
      }));
    }
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
