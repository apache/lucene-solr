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

import { Component, OnInit } from '@angular/core';
import { NgxGraphModule } from '@swimlane/ngx-graph';
import * as table from '@angular/material/table';
import {Observable,of, from } from 'rxjs';
import { CloudGraphService } from '../../services/solr-cloud-graph/cloud-graph.service';
import { SystemService } from '../../services/solr-admin-info-system/system.service';
import { CloudSystemResponse } from '../../domain/solr-admin-info-system/cloud-system-response';
import { SolrSystemResponse } from '../../domain/solr-admin-info-system/solr-system-response';
import {MatTableModule} from '@angular/material/table';
import { ChildActivationStart } from '@angular/router';
import { SolrAdminMetricsService } from '../../services/solr-admin-metrics/solr-admin-metrics.service';
import { JvmData } from '../../domain/solr-admin-info-system/jvm-data';
import { BytesPipe } from 'angular-pipes';
import {NestedTreeControl} from '@angular/cdk/tree';
import {MatTreeNestedDataSource, MatTreeModule} from '@angular/material/tree';

var core = {};
interface SolrPrimitive {
  name: string;
  children?: SolrPrimitive[];
  core?: Object;
}

@Component({
  selector: 'app-cloud-graph',
  templateUrl: './cloud-graph.component.html',
  styleUrls: ['./cloud-graph.component.scss']
})
export class CloudGraphComponent implements OnInit {
  clusterNodes: string[];
  clusterNodesObj;
  clusterNodesParam: string;
  cloudGraphData;
  displayedColumns: string[] = ['node']; //, 'CPU', 'Heap', 'Disk usage', 'Requests', 'Collections', 'Replicas'];
  aggregateClusterData;
  arrayOfNodeNames: any[];
  jvmData: any;
  arrayOfJvmNodeNames: any[];
  zkStatus;
  cloudStatus;
  cloudNodeGraphData;
  nestedTreeControl;
  nestedDataSource = new MatTreeNestedDataSource<SolrPrimitive>();

  constructor(private cloudGraphService: CloudGraphService, private systemService: SystemService, 
    private cloudJvmMetricsService: SolrAdminMetricsService, public table: MatTableModule, public tree: MatTreeModule)  {
      
     }

  ngOnInit() {
    this.nestedTreeControl = new NestedTreeControl<SolrPrimitive>(this.getChildren);
    this.nestedDataSource = new MatTreeNestedDataSource();
    this.cloudGraphService.get().subscribe(
      response => {
        this.aggregateClusterData = response;
        this.prepDataForNodeTree(this.aggregateClusterData);
        this.clusterNodes = response.cluster["live_nodes"];
        this.objectifyClusterNodes(this.clusterNodes);
        this.clusterNodesParam = this.clusterNodes.join();
        this.handleIniitalResponse(this.clusterNodesParam);
        this.handleJvmMetricsCall(this.clusterNodesParam);
        this.handleCloudStatusCall();
        return this.nestedDataSource;
      },
      err => {
          console.error(err);
      }
    );
  }
  handleIniitalResponse(clusterNodesParam){
    this.systemService.getCloudData(clusterNodesParam).subscribe(
      response => {
        this.displayedColumns;
        delete response.responseHeader;
        this.cloudGraphData = response;
        this.processCloudGraphData(this.cloudGraphData);
      },
      err => {
        console.error(err);
      }
    );
  }
  handleJvmMetricsCall(clusterNodesParam){
    this.cloudJvmMetricsService.getCloudJvmData(clusterNodesParam).subscribe(
      response => {
        this.jvmData = response;
        delete response.responseHeader;
        this.processCloudJvmGraphData(this.jvmData);
        this.zkStatus = this.handleZkStatusCall();
      },
      err => {
        console.error(err);
      }
    );
  }

  handleZkStatusCall(){
    this.cloudGraphService.getZkStatus().subscribe(
      response => {
        this.zkStatus = response;
        delete response.responseHeader;
        return this.zkStatus;
      },
      err => {
        console.error(err);
      }
    );
  }
  
  handleCloudStatusCall(){
    this.cloudGraphService.getCloudStatus().subscribe(
      response => {
        this.cloudStatus = response;
        delete response.responseHeader;
        return this.cloudStatus;
      },
      err => {
        console.error(err);
      }
    );
  } 


  processCloudGraphData(cloudGraphData: any){
    let data = this.cloudGraphData;
    this.arrayOfNodeNames  = []; //will be array of node names
    let keys = Object.keys(data); /// node names
    for(var i = 0, j = keys.length; i < j; i++){
      var key  = keys[i]; 
      this.arrayOfNodeNames.push({"node": key, "details": data[key]});
    
    }
    return this.arrayOfNodeNames;
  }

  processCloudJvmGraphData(jvmData: any){
    let data = this.jvmData;
    this.arrayOfJvmNodeNames = []; //will be array of node names
    let keys = Object.keys(data); /// node names
    for(var i = 0, j = keys.length; i < j; i++){
      var key  = keys[i]; 
      this.arrayOfNodeNames[i]["metrics"]  = data[key]["metrics"]["solr.jetty"]["org.eclipse.jetty.server.handler.DefaultHandler.get-requests"];
      this.arrayOfNodeNames[i]["metrics"]["1minRate"] = this.trimRPM(this.arrayOfNodeNames[i]["metrics"]["1minRate"]) || "";
    }
    return this.arrayOfJvmNodeNames;
  }

  trimRPM(data: any){
    let rpmData = data.toFixed(1);
    return rpmData;
  }
  objectifyClusterNodes(clusterNodes: string[]){
    this.clusterNodesObj = {};
    for(let i = 0, j = clusterNodes.length; i < j; i++){
    this.clusterNodesObj[i] = {"node": this.clusterNodes[i]};
    } 
    return this.clusterNodesObj;
  }
   

  prepDataForNodeTree(aggregateClusterData){
    var data = aggregateClusterData.cluster.collections;
    
    var collectionsArray = []; // 
    // Shard
    var shardsArrayed  = function(collection){
        var shardsArray = [];
        Object.keys(collection).map(function(k) { 
        let newKey ="";
        newKey = k;
        if(collection[newKey]["replicas"]) {
            replicasArrayed(collection[newKey]["replicas"]);
            }
        let newObject = {};
        newObject["name"] = k;
        newObject[newKey] = collection[k];
        newObject["children"] = replicasArrayed(collection[newKey]["replicas"])
        delete newObject[newKey];
        shardsArray.push(newObject);
      });
        return shardsArray;
    }
    ​
    // Replica
    ​
    var replicasArrayed  = function(shard){
        var replicasArray = [];
        Object.keys(shard).map(function(k) { 
        let newKey ="";
        newKey = k;
        let newObject = {};
        newObject["name"] = k;
        newObject["children"] = Object.values(shard[k]);
        // newObject[newKey] = shard[k];
        replicasArray.push(newObject);
      });
        return replicasArray;
    }
    ​
    var array = Object.keys(data).map(function(k) {
        
        var newKey ="";
        newKey = k;
        if(data[newKey]["shards"]) {
          shardsArrayed(data[newKey]["shards"]);
        }
        var newObject = {};
        newObject["name"] = k;
        newObject["children"] = [];
        newObject[newKey] = data[k];
        newObject["children"] = shardsArrayed(data[newKey]["shards"]);
        //newObject[newKey]["children"] = shardsArrayed(data[newKey]["shards"]);
     ​   delete newObject[newKey];
        collectionsArray.push(newObject);
        
    });
    this.aggregateClusterData = collectionsArray;
    this.nestedDataSource.data = this.aggregateClusterData;
    return this.nestedDataSource;
  }
 

  //process
  hasChild = (_: number, node: SolrPrimitive) => !!node.children && node.children.length > 0;
  getChildren = (node: SolrPrimitive) => of(node.children);
}