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

import {SystemInfo, ParsedMemory} from './systemInfo';
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { of } from 'rxjs/observable/of';
import { catchError, map, tap } from 'rxjs/operators';

@Injectable()
export class SolrService {

  // TODO: nocommit
  private noCommitBaseUrl = 'http://localhost:8983';
  private noCommitAuthHeader = ('Basic ' + btoa('user:password'));

  private httpOptions = {
    headers: new HttpHeaders({'Authorization' : this.noCommitAuthHeader})
  };

  private baseUrl = this.noCommitBaseUrl;
  private systemInfoUrl = this.baseUrl + '/solr/admin/info/system';

  constructor(private http: HttpClient) {
  }

  getSystemInfo (): Observable<SystemInfo> {
    return this.http.get<SystemInfo>(this.systemInfoUrl).pipe(map(si => {

      // load average
      const loadAverageArr = (si.system.uptime || '' ).match( /load averages?: (\d+[.,]\d\d),? (\d+[.,]\d\d),? (\d+[.,]\d\d)/ );
      const loadAverageNumber: number[] = [0, 0, 0];
      if (loadAverageArr) {
        for (let i = 1; i < 4; i++) {
          loadAverageNumber[i - 1] = Number(loadAverageArr[i].replace(',', '.')); // for European users
        }
      }
      si.system.load_average = loadAverageNumber;

      // physical memory
      const parsedMemory = new ParsedMemory();
      parsedMemory.max = this.parseMemoryValue(si.system.totalPhysicalMemorySize);
      parsedMemory.total = this.parseMemoryValue(si.system.totalPhysicalMemorySize - si.system.freePhysicalMemorySize);
      parsedMemory.percentage = (parsedMemory.total / parsedMemory.max * 100).toFixed(1) + '%';
      parsedMemory.maxDisplay = this.prettyPrintBytes(parsedMemory.max);
      parsedMemory.totalDisplay = this.prettyPrintBytes(parsedMemory.total);
      si.system.memory = parsedMemory;

      // swap space
      const parsedSwap = new ParsedMemory();
      parsedSwap.max = this.parseMemoryValue(si.system.totalSwapSpaceSize);
      parsedSwap.total = this.parseMemoryValue(si.system.totalSwapSpaceSize - si.system.freeSwapSpaceSize);
      parsedSwap.percentage = (parsedSwap.total / parsedSwap.max * 100).toFixed(1) + '%';
      parsedSwap.maxDisplay = this.prettyPrintBytes(parsedSwap.max);
      parsedSwap.totalDisplay = this.prettyPrintBytes(parsedSwap.total);
      si.system.swap = parsedSwap;

      // file handles
      si.system.fileDescriptorPercentage = (si.system.openFileDescriptorCount / si.system.maxFileDescriptorCount * 100).toFixed(1) + '%';

      // java memory
      const parsedJvmTotalMemory = new ParsedMemory();
      parsedJvmTotalMemory.max = this.parseMemoryValue(si.jvm.memory.raw.max);
      parsedJvmTotalMemory.maxDisplay = this.prettyPrintBytes(parsedJvmTotalMemory.max);
      parsedJvmTotalMemory.total = this.parseMemoryValue(si.jvm.memory.raw.total);
      parsedJvmTotalMemory.totalDisplay = this.prettyPrintBytes(parsedJvmTotalMemory.total);
      parsedJvmTotalMemory.percentage = (parsedJvmTotalMemory.total / parsedJvmTotalMemory.max * 100).toFixed(1) + '%';
      si.system.jvmTotal = parsedJvmTotalMemory;

      const parsedJvmUsedMemory = new ParsedMemory();
      parsedJvmUsedMemory.total = this.parseMemoryValue(si.jvm.memory.raw.used);
      parsedJvmUsedMemory.totalDisplay = this.prettyPrintBytes(parsedJvmUsedMemory.total);
      parsedJvmUsedMemory.percentage = (parsedJvmUsedMemory.total / parsedJvmTotalMemory.total * 100).toFixed(1) + '%';
      si.system.jvmUsed = parsedJvmUsedMemory;

      si.system.jvmMemoryPercentage = (parsedJvmUsedMemory.total / parsedJvmTotalMemory.max * 100).toFixed(1) + '%';

       // no info bar:
      si.system.noInfo = !(
        si.system.totalPhysicalMemorySize && si.system.freePhysicalMemorySize &&
        si.system.totalSwapSpaceSize && si.system.freeSwapSpaceSize &&
        si.system.openFileDescriptorCount && si.system.maxFileDescriptorCount);

      // command line args:
      si.jvm.jmx.commandLineArgs = si.jvm.jmx.commandLineArgs.sort();

      return si;
    }));
  }

  private parseMemoryValue ( value: number ) {
    if (value !== Number( value )) {
      const units = 'BKMGTPEZY';
      const match = value.toString().match( /^(\d+([,\.]\d+)?) (\w).*$/ );
      const parsedValue = parseFloat( match[1] ) * Math.pow( 1024, units.indexOf( match[3].toUpperCase() ) );
      return parsedValue;
    }
    return value;
  }
  private prettyPrintBytes (byteValue: number) {
    let unit = null;
    byteValue /= 1024;
    byteValue /= 1024;
    unit = 'MB';
    if (1024 <= byteValue) {
      byteValue /= 1024;
      unit = 'GB';
    }
    return byteValue.toFixed( 2 ) + ' ' + unit;
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(error);
      return of(result as T);
    };
  }
}
