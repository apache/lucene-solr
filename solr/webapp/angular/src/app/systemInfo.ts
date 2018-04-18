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

export class SystemInfo {
  responseHeader: ResponseHeader;
  mode: string;
  zkHost: string;
  solr_home: string;
  lucene: Versions;
  jvm: Jvm;
  system: SystemMetadata;
}
export class ResponseHeader {
  status: number;
  QTime: number;
}
export class Versions {
  'solr-spec-version': string;
  'solr-impl-version': string;
  'lucene-spec-version': string;
  'lucene-impl-version': string;
}
export class Jvm {
  version: string;
  name: string;
  spec: SpecOrVm;
  jre: Jre;
  vm: SpecOrVm;
  processors: number;
  memory: Memory;
  jmx: Jmx;
}
export class SpecOrVm {
  vendor: string;
  name: string;
  version: string;
}
export class Jre {
  vendor: string;
  version: string;
}
export class Memory {
  free: string;
  total: string;
  max: string;
  used: string;
  raw: RawMemory;
}
export class RawMemory {
  free: number;
  total: number;
  max: number;
  used: number;
  'used%': number;
}
export class ParsedMemory {
  max: number;
  total: number;
  percentage: string;
  maxDisplay: string;
  totalDisplay: string;
}
export class Jmx {
  bootclasspath: string;
  classpath: string;
  commandLineArgs: (string)[];
  startTime: string;
  upTimeMS: number;
}
export class SystemMetadata {
  name: string;
  arch: string;
  availableProcessors: number;
  systemLoadAverage: number;
  version: string;
  committedVirtualMemorySize: number;
  freePhysicalMemorySize: number;
  freeSwapSpaceSize: number;
  processCpuLoad: number;
  processCpuTime: number;
  systemCpuLoad: number;
  totalPhysicalMemorySize: number;
  totalSwapSpaceSize: number;
  maxFileDescriptorCount: number;
  openFileDescriptorCount: number;
  uname: string;
  uptime: string;
  load_average: number[];
  memory: ParsedMemory;
  swap: ParsedMemory;
  jvmTotal: ParsedMemory;
  jvmUsed: ParsedMemory;
  jvmMemoryPercentage: string;
  fileDescriptorPercentage: string;
  noInfo = false;
}
