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
  reloadSuccess = false;
  reloadFailure = false;
  addMessage = null;
  swapMessage = null;
  deleteMessage = null;
  createReplicaMessage = null;
  collectionDeleteConfirm = null;
  aliasToCreate = null;
  aliasCollections: string[] = [];
  aliasToDelete = null;

  aliases: string[] = [];
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
    this.zookeeperService.listAliases().subscribe(a => {
      this.aliases = a;
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
    if(!this.sharedService.loaded) {
      return;
    }
    this.sharedService.clearErrors();
    if (!this.newCollection.name) {
      this.addMessage = "Please provide a collection name";
    } else {
      this.sharedService.loaded = false;
      this.collectionsService.addCollection(this.newCollection).subscribe(c => {
        this.refresh();
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
    if(!this.sharedService.loaded || !this.collection) {
      return;
    }
    this.sharedService.clearErrors();
    this.sharedService.loaded = false;
    this.collectionsService.reloadCollection(this.collection.name).subscribe(c => {
      this.reloadSuccess = true;
      this.reloadFailure = false;
      this.sharedService.loaded = true;
    }, (error => {
      this.sharedService.showError(error);
      this.reloadSuccess = false;
      this.reloadFailure = true;
      this.sharedService.loaded = true;
    }));
  }

  toggleCreateAlias() {
    this.showCreateAlias = !this.showCreateAlias;
  }

  toggleDeleteAlias() {
    this.showDeleteAlias = !this.showDeleteAlias;
  }

  createAlias() {
    this.sharedService.loaded = false;
    this.collectionsService.createAlias(this.aliasToCreate, this.aliasCollections).subscribe(c => {
      this.aliasToCreate = null;
      this.aliasCollections = [];
      this.showCreateAlias = false;
      this.refresh();
      this.sharedService.loaded = true;
    }, (error => {
      this.sharedService.showError(error);
      this.sharedService.loaded = true;
    }));
  }

  cancelCreateAlias() {
    this.aliasToCreate = null;
    this.aliasCollections = [];
    this.showCreateAlias = false;
  }

  deleteAlias() {
    this.sharedService.loaded = false;
    this.collectionsService.deleteAlias(this.aliasToDelete).subscribe(c => {
      this.aliasToDelete = null;
      this.showDeleteAlias = false;
      this.refresh();
      this.sharedService.loaded = true;
    }, (error => {
      this.sharedService.showError(error);
      this.sharedService.loaded = true;
    }));
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
    if(!this.sharedService.loaded || !this.collection) {
      return;
    }
    if(this.collection.name != this.collectionDeleteConfirm) {
      this.deleteMessage = "Collection names do not match.";
      return;
    }
    this.sharedService.clearErrors();
    this.sharedService.loaded = false;
    this.collectionsService.deleteCollection(this.collection.name).subscribe(c => {
      this.refresh();
      this.sharedService.loaded = true;
      this.deleteMessage = null;
      this.collectionDeleteConfirm = null;
      this.collection = null;
      this.showDelete = null;
    }, (error => {
      this.sharedService.showError(error);
      this.deleteMessage = null;
      this.collectionDeleteConfirm = null;
      this.showDelete = null;
      this.sharedService.loaded = true;
    }));
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
