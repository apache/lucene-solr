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
import { Collection } from './collections/collections.component';
import { Core } from './cores/cores.component';

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
    showPing=true;
    pingMS = 1234567890;

    addCollection(c: Collection) {
      this.collections.push(c.name);
      this.collections.sort();
    }

    addCore(c: Core) {
      this.cores.push(c.name);
      this.cores.sort();
    }

    setCollections(cArr: String[]) {
      this.collections = cArr;
      this.collections.sort();
    }

    setCores(cArr: String[]) {
      this.cores = cArr;
      this.cores.sort();
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
