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
import { ThreadDumpService } from '../../services/solr-thread-dump/thread-dump.service';
import { ThreadDumpResponse } from 'src/app/domain/solr-thread-dump/thread-dump-response';
import {FlatTreeControl} from '@angular/cdk/tree';
// import {MatExpansionModule} from '@angular/material/expansion';
@Component({
  selector: 'thread-dump-component',
  templateUrl: './thread-dump.component.html',
  styleUrls: ['./thread-dump.component.scss']
})
export class ThreadDumpComponent implements OnInit {
panelOpenState = false;
threadDumpData;

  constructor(private threadDumpService: ThreadDumpService) { }

  ngOnInit() {
    this.threadDumpService.getData().subscribe(
      response => {
        this.threadDumpData = response["system"]["threadDump"];
        this.processThreadDump(this.threadDumpData);
      },
      err => {
          console.error(err);
      }
    );
  }
  processThreadDump(threadDumpData: any): any{
    this.threadDumpData = threadDumpData.filter(function(element, index){
      return index % 2 == 1
    })
  }
}
