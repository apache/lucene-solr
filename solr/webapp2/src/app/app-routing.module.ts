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
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { DashboardComponent } from './pages/dashboard/dashboard.component';
import { CloudConfigsComponent } from './pages/cloud-configs/cloud-configs.component';
import { CloudGraphComponent } from './pages/cloud-graph/cloud-graph.component';
import { CollectionsComponent } from './pages/collections/collections.component';
import { JavaPropsComponent } from './pages/java-props/java-props.component';
import { LoggingComponent } from './pages/logging/logging.component';
import { ThreadDumpComponent } from './pages/thread-dump/thread-dump.component';

const routes: Routes = [
    { path: '', redirectTo: 'index', pathMatch: 'full' },
    { path: 'index', component: DashboardComponent },
    { path: 'cloud-configs', component: CloudConfigsComponent },
    { path: 'cloud-graph', component: CloudGraphComponent },
    { path: 'collections', component: CollectionsComponent },
    { path: 'java-props', component: JavaPropsComponent },
    { path: 'logging', component: LoggingComponent },
    { path: 'thread-dump', component: ThreadDumpComponent },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
