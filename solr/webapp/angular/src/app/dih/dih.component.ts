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
import { SharedService } from '../shared.service';
import { DihService, DihInfo } from '../dih.service';
import { MbeansService } from '../mbeans.service';


@Component({
  selector: 'app-dih',
  templateUrl: './dih.component.html'
})
export class DihComponent implements OnInit {
  sharedService: SharedService;
  collectionName: string;
  handler: string;
  handlers: string[];
  entities: string[];
  hasHandlers = false;
  path: string;
  config: string;
  rawResponse: string;
  info: DihInfo;
  lastUpdate: string;
  lastUpdateUTC: string;
  isAborting = false;
  isRunning = false;
  reloaded = false;
  isDebugMode = false;
  showRawStatus = false;
  showConfiguration = false;
  showRawDebug = false;
  isStatusLoading = false;
  statusUpdated = false;
  autorefresh = false;

  form_command: string;
  form_commandverbose: boolean;
  form_clean: boolean;
  form_commit: boolean;
  form_showDebug: boolean;
  form_entity: string;
  form_start: number;
  form_rows: number;
  form_custom: string;

  constructor(private route: ActivatedRoute,
    sharedService: SharedService,
    private mbeansService: MbeansService,
    private dihService: DihService) {
    this.sharedService = sharedService;
  }

  refresh() {
    this.isAborting = false;
    this.isRunning = false;
    this.reloaded = false;
    this.isDebugMode = false;
    this.showRawStatus = false;
    this.showConfiguration = false;
    this.showRawDebug = false;
    this.isStatusLoading = false;
    this.statusUpdated = false;
    this.autorefresh = false;
    this.rawResponse = null;
    this.form_command = null;
    this.form_commandverbose = false;
    this.form_clean = false;
    this.form_commit = false;
    this.form_showDebug = false;
    this.form_entity = null;
    this.form_start = null;
    this.form_rows = null;
    this.form_custom = null;

    this.handlers = [];
    this.entities = [];
    this.mbeansService.data(this.collectionName).subscribe(mbeans => {
      let i = 0;
      for (let mbean in mbeans) {
        if (i % 2 == 1) {
          const mb: any = mbeans[mbean];
          for (let key in mb) {
            const value = mb[key];
            if (value.class) {
              if (value.class === 'org.apache.solr.handler.dataimport.DataImportHandler') {
                this.handlers.push(key);
              }
            }
          }
        }
        i++;
      }
      this.hasHandlers = this.handlers.length > 0;
      let foundHandler = false;
      if (this.hasHandlers) {
        if (this.handler) {
          for (let index in this.handlers) {
            const h = this.handlers[index];
            if (this.handler == h) {
              foundHandler = true;
              break;
            }
            if (h.length > 1 && h.startsWith("/") && !this.handler.startsWith("/")) {
              let withSlash = "/" + this.handler;
              if (withSlash == h) {
                this.handler = withSlash;
                foundHandler = true;
                break;
              }
            }
          }
        }
        if (!foundHandler) {
          this.handler = this.handlers[0];
        }
        this.path = "/" + this.collectionName + (this.handler.startsWith("/") ? "" : "/") + this.handler;
        this.runDih("status");
        this.refreshConfig();
      }
    }, (error => {
      this.sharedService.showError(error);
      return;
    }));
    const now = new Date();
    this.lastUpdate = now.toTimeString().split(' ').shift();
    this.lastUpdateUTC = now.toUTCString();
  }

  runDih(command: string, callback?: Function) {
    this.dihService.dih(this.path, command, this.isDebugMode).subscribe(info => {
      this.info = info;
      if (callback) {
        callback();
      }
    }, (error => {
      this.sharedService.showError(error);
      return;
    }));
  }

  refreshConfig() {
    this.dihService.config(this.path).subscribe(config => {
      this.config = config;
    });
  }

  refreshStatus() {
    this.isStatusLoading = true;
    this.statusUpdated = false;
    const self = this;
    this.runDih("status", function() { self.isStatusLoading = false; self.statusUpdated = true; });
  }

  abort() {
    this.isAborting = true;
    const self = this;
    this.runDih("abort", function() { self.isAborting = false; });
  }
  updateAutoRefresh() {
    this.autorefresh = !this.autorefresh;
  }
  toggleRawStatus() {
    this.showRawStatus = !this.showRawStatus;
  }
  toggleConfiguration() {
    this.showConfiguration = !this.showConfiguration;
  }
  toggleDebug() {
    this.isDebugMode = !this.isDebugMode;
  }
  toggleRawDebug() {
    this.showRawDebug = !this.showRawDebug;
  }
  reload() {
    this.reloaded = false;
    const self = this;
    this.runDih("reload", function() { self.reloaded = true; });
    this.refreshConfig();
  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.collectionName = params['name'];
      this.handler = params['handler'];
      this.refresh();
    });
  }


}
