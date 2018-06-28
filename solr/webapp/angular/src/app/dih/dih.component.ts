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

import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { SharedService } from '../shared.service';
import { DihService, DihInfo } from '../dih.service';
import { MbeansService } from '../mbeans.service'; import { Observable } from "rxjs";
import { TimerObservable } from "rxjs/observable/TimerObservable";
import 'rxjs/add/operator/takeWhile';


@Component({
  selector: 'app-dih',
  templateUrl: './dih.component.html'
})
export class DihComponent implements OnInit, OnDestroy {
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

  clearIndicators() {
    this.reloaded = false;
    this.isAborting = false;
    this.isStatusLoading = false;
    this.statusUpdated = false;
    this.isRunning = false;
  }

  refresh() {
    this.clearIndicators();
    this.isDebugMode = false;
    this.showRawStatus = false;
    this.showConfiguration = false;
    this.showRawDebug = false;
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
        this.runDih(new Map([["command", "status"]]));
        this.refreshConfig();
      }
    }, (error => {
      this.sharedService.showError(error);
      return;
    }));
    this.refreshLastUpdated();
  }

  refreshLastUpdated() {
    const now = new Date();
    this.lastUpdate = now.toTimeString().split(' ').shift();
    this.lastUpdateUTC = now.toUTCString();
  }

  runDih(parameters: Map<string, string>, callback?: Function, usePost: boolean = false) {
    this.dihService.dih(this.path, parameters, usePost).subscribe(info => {
      this.info = info;
      this.refreshLastUpdated();
      if (callback) {
        callback();
      }
    }, (error => {
      this.sharedService.showError(error);
      return;
    }));
  }

  refreshConfig() {
    this.clearIndicators();
    this.dihService.config(this.path).subscribe(config => {
      this.config = config;
      this.entities = [];
      try {
        const parser = new DOMParser();
        const xmlDoc = parser.parseFromString(this.config, "text/xml");
        const elems: NodeList = xmlDoc.getElementsByTagName("entity");
        for (let i = 0; i < elems.length; i++) {
          this.entities.push(elems[i].attributes.getNamedItem("name").value);
        }
      } catch (e) {
        this.sharedService.showError(e);
      }
    });
  }

  refreshStatus() {
    this.clearIndicators();
    this.isStatusLoading = true;
    const self = this;
    this.runDih(new Map([["command", "status"]]), function() {
      self.isStatusLoading = false;
      self.statusUpdated = true;
      self.isRunning = self.info.status == "busy";
    });
  }
  submit() {
    if (!this.form_command) {
      return;
    }
    this.clearIndicators();
    const m = new Map();
    m.set("command", this.form_command);
    m.set("verbose", this.form_commandverbose ? "true" : "false");
    m.set("clean", this.form_clean ? "true" : "false");
    m.set("commit", this.form_commit ? "true" : "false");
    m.set("debug", this.form_showDebug ? "true" : "false");
    if (this.form_entity) {
      m.set("entity", this.form_entity);
    }
    if (this.form_start) {
      m.set("start", this.form_start);
    }
    if (this.form_rows) {
      m.set("rows", this.form_rows);
    }
    if (this.form_custom) {
      const customParams = this.form_custom.split("&");
      for (let i in customParams) {
        const parts = customParams[i].split("=");
        m.set(parts[0], parts[1]);
      }
    }
    if (this.isDebugMode) {
      m.set("dataConfig", this.config);
    }
    this.isRunning = true;
    const self = this;
    this.runDih(m, function() {
      self.isRunning = false;
      self.rawResponse = self.info.rawStatus;
    }, true);
  }
  abort() {
    this.clearIndicators();
    this.isAborting = true;
    const self = this;
    this.runDih(new Map([["command", "abort"]]), function() { self.isAborting = false; });
  }
  updateAutoRefresh() {
    this.autorefresh = !this.autorefresh;
    if (this.autorefresh) {
      TimerObservable.create(0, 2000)
        .takeWhile(() => this.autorefresh)
        .subscribe(() => {
          this.refreshStatus();
        });
    }
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
    this.clearIndicators();
    const self = this;
    this.runDih(new Map([["command", "reload"]]), function() { self.reloaded = true; });
    this.refreshConfig();
  }
  ngOnInit() {
    this.route.params.subscribe(params => {
      this.collectionName = params['name'];
      this.handler = params['handler'];
      this.refresh();
    });
  }
  ngOnDestroy() {
    this.autorefresh = false;
  }

}
