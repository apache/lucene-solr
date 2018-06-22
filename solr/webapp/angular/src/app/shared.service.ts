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

import { Injectable} from '@angular/core';
import { CollectionsService } from './collections.service';
import { CoresService } from './cores.service';

@Injectable()
export class SharedService {
    loaded = true;
    exceptions = []; //{msg: 'test exception'}, {msg: 'another one'}];
    showInitFailures = false; //true;
    initFailures = []; //{core: 'sample core', error: 'sample error' }];
    connectionRecovered = true;

    showingLogging = false;
    showingCloud = false;
    isCloudEnabled = true;
    collections = [];
    cores = [];
    currentCollection = null;
    currentCore = null;

    constructor(private collectionsService: CollectionsService, private coresService: CoresService) {}

    refreshCollections() {
      this.collectionsService.listCollections().subscribe(c => {
        this.collections = c;
        this.collections.sort();
      });
    }

    refreshCores() {
      this.coresService.listCores().subscribe(c => {
        this.cores = c;
        this.cores.sort();
      });
    }

    clearErrors() {
      this.exceptions = [];
    }

    showError(e: any) {
      if(e.error) {
        if(e.error.exception) {
            if(e.error.exception.msg) {
              this.exceptions.push(e.error.exception.msg);
            } else {
              this.exceptions.push(JSON.stringify(e.error.exception));
            }
        } else if(e.error.msg) {
          this.exceptions.push(e.error.msg);
        } else {
            this.exceptions.push(JSON.stringify(e.error));
        }
      } else {
        this.exceptions.push(JSON.stringify(e));
      }
    }
}

export class InitFailure {
    core: String;
    error: String;
    constructor(core, error) {
        this.core = core;
        this.error = error;
    }
}
