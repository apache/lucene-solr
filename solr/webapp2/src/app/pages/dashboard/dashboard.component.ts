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
import { SystemService } from '../../services/solr-admin-info-system/system.service';
import { SolrSystemResponse } from '../../domain/solr-admin-info-system/solr-system-response';
import { MatDialog } from '@angular/material/dialog';
import { SolrVersionDialogComponent } from '../../dialogs/solr-version-dialog/solr-version-dialog.component';
import { SystemData } from '../../domain/solr-admin-info-system/system-data';
import { JvmData } from '../../domain/solr-admin-info-system/jvm-data';
import { InfoDialogComponent } from '../../dialogs/info-dialog/info-dialog.component';
import { BytesPipe } from 'angular-pipes';
import { SolrAdminMetricsService } from '../../services/solr-admin-metrics/solr-admin-metrics.service';

@Component({
    selector: 'app-dashboard',
    templateUrl: './dashboard.component.html',
    styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit {
    data: SolrSystemResponse;
    systemData: SystemData;
    jvmData: JvmData;

    jvmMetrics: any;
    jvmMetricsGrouped: any;

    // System memory
    totalPhysicalMemory: number;
    usedPhysicalMemory: number;
    usedPhysicalMemoryPercent: number;
    usedSwapSpace: number;
    usedSwapSpacePercent: number;

    // CPU
    cpuUtilization: number;
    processCpuUtilization: number;

    // Files
    fileDescriptorUtilization: number;

    // GC
    gcGroups = [];

    // JVM Memory
    jvmMemoryChartData;

    // Initialization
    constructor(private systemService: SystemService, private metrics: SolrAdminMetricsService, public dialog: MatDialog) { }

    ngOnInit() {
        this.systemService.getData().subscribe(
            response => {
                this.handleResponse(response);
            },
            err => {
                console.error(err);
            }
        );
        this.metrics.getJvmData().subscribe(
            response => {
                this.jvmMetrics = response.metrics['solr.jvm'];
                this.jvmMetricsGrouped = this.flatToGroups(this.jvmMetrics);
                this.processJvmMetrics(this.jvmMetrics);
            },
            err => { console.error(err); }
        );
    }
    processJvmMetrics(jvmMetrics: any): any {
        this.cpuUtilization = 100 * (jvmMetrics['os.systemCpuLoad'] / jvmMetrics['os.availableProcessors']);
        this.totalPhysicalMemory = jvmMetrics['os.totalPhysicalMemorySize'];
        this.usedPhysicalMemory = jvmMetrics['os.totalPhysicalMemorySize'] - jvmMetrics['os.freePhysicalMemorySize'];
        this.usedPhysicalMemoryPercent = 100 * (this.usedPhysicalMemory / jvmMetrics['os.totalPhysicalMemorySize']);
        this.fileDescriptorUtilization = 100 * (jvmMetrics['os.openFileDescriptorCount'] / jvmMetrics['os.maxFileDescriptorCount']);
        this.usedSwapSpace = jvmMetrics['os.totalSwapSpaceSize'] - jvmMetrics['os.freeSwapSpaceSize'];
        this.usedSwapSpacePercent = 100 * (this.usedSwapSpace / jvmMetrics['os.totalSwapSpaceSize']);
        this.processCpuUtilization = 100 * (jvmMetrics['os.processCpuLoad'] / jvmMetrics['os.availableProcessors']);
        this.processGCData();
        this.processJvmMemory();
    }

    processGCData(): any {
        const gc = this.jvmMetricsGrouped.gc;
        Object.getOwnPropertyNames(gc).forEach(gcKey => {
            this.gcGroups.push({
                name: gcKey,
                count: gc[gcKey].count,
                time: gc[gcKey].time
            });
        });
    }

    processJvmMemory() {
        this.jvmMemoryChartData = [{
            name: 'heap',
            series: [{
                name: 'committed',
                value: this.jvmMetricsGrouped.memory.heap.committed
            }, {
                name: 'init',
                value: this.jvmMetricsGrouped.memory.heap.init
            }, {
                name: 'max',
                value: this.jvmMetricsGrouped.memory.heap.max
            }, {
                name: 'used',
                value: this.jvmMetricsGrouped.memory.heap.used
            }]
        }, {
            name: 'non-heap',
            series: [{
                name: 'committed',
                value: this.jvmMetricsGrouped.memory['non-heap'].committed
            }, {
                name: 'init',
                value: this.jvmMetricsGrouped.memory['non-heap'].init
            }, {
                name: 'max',
                value: this.jvmMetricsGrouped.memory['non-heap'].max
            }, {
                name: 'used',
                value: this.jvmMetricsGrouped.memory['non-heap'].used
            }]
        }, {
            name: 'total',
            series: [{
                name: 'committed',
                value: this.jvmMetricsGrouped.memory.total.committed
            }, {
                name: 'init',
                value: this.jvmMetricsGrouped.memory.total.init
            }, {
                name: 'max',
                value: this.jvmMetricsGrouped.memory.total.max
            }, {
                name: 'used',
                value: this.jvmMetricsGrouped.memory.total.used
            }]
        }];
    }

    flatToGroups(object: any): any {
        const result = {};
        this.eachKeyValue(object, (namespace, value) => {
            const parts: String[] = namespace.split('.');
            const last = parts.pop();
            let node = result;
            parts.forEach((key) => {
                node = node[key.valueOf()] = node[key.valueOf()] || {};
            });
            node[last.valueOf()] = value;
        });
        return result;
    }

    eachKeyValue(obj, callback) {
        for (const i in obj) {
            if (obj.hasOwnProperty(i)) {
                callback(i, obj[i]);
            }
        }
    }

    handleResponse(data: SolrSystemResponse) {
        this.data = data;
        this.processSystemData(data.system);
        this.processJvmData(data.jvm);
    }
    processJvmData(jvm: JvmData): any {
        this.jvmData = jvm;
    }
    processSystemData(system: SystemData): any {
        this.systemData = system;
    }

    // Formatters
    valueAsPercent(value) {
        return `${Math.round(value).toLocaleString()}%`;
    }

    valueAsMemorySize(value) {
        return new BytesPipe().transform(value);
    }

    valueAsMS(value) {
        return `${Math.round(value).toLocaleString()}ms`;
    }

    // Event Handlers
    openSolrVersionDialog(): void {
        this.dialog.open(SolrVersionDialogComponent, {
            data: this.data
        });
    }


    openJVMDialog(): void {
        this.dialog.open(InfoDialogComponent, {
            data: {
                title: 'JVM Information',
                content: this.jvmData.name + ' v' + this.jvmData.version
            }
        });
    }

}
