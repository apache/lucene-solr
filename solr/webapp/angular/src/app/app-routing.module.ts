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

import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard.component';
import { CollectionsComponent } from './collections/collections.component';
import { CoresComponent } from './cores/cores.component';
import { AnalysisComponent } from './analysis/analysis.component';
import { DihComponent } from './dih/dih.component';
import { DocumentsComponent } from './documents/documents.component';
import { FilesComponent } from './files/files.component';
import { QueryComponent } from './query/query.component';
import { StreamComponent } from './stream/stream.component';
import { SchemaComponent } from './schema/schema.component';
import { PluginsComponent } from './plugins/plugins.component';
import { ReplicationComponent } from './replication/replication.component';
import { SegmentsComponent } from './segments/segments.component';

const routes: Routes = [
  { path: 'dashboard', component: DashboardComponent },
  { path: 'collections', component: CollectionsComponent },
  { path: 'collections/:name', component: CollectionsComponent },
  { path: 'collections/:name/overview', component: CollectionsComponent },
  { path: 'collections/:name/analysis', component: AnalysisComponent },
  { path: 'collections/:name/dataimport', component: DihComponent },
  { path: 'collections/:name/documents', component: DocumentsComponent },
  { path: 'collections/:name/files', component: FilesComponent },
  { path: 'collections/:name/query', component: QueryComponent },
  { path: 'collections/:name/stream', component: StreamComponent },
  { path: 'collections/:name/schema', component: SchemaComponent },
  { path: 'cores', component: CoresComponent },
  { path: 'cores/:name/overview', component: CoresComponent },
  { path: 'cores/:name/analysis', component: AnalysisComponent },
  { path: 'cores/:name/dataimport', component: DihComponent },
  { path: 'cores/:name/documents', component: DocumentsComponent },
  { path: 'cores/:name/plugins', component: PluginsComponent },
  { path: 'cores/:name/query', component: QueryComponent },
  { path: 'cores/:name/replication', component: ReplicationComponent },
  { path: 'cores/:name/schema', component: SchemaComponent },
  { path: 'cores/:name/segments', component: SegmentsComponent },
  { path: '', redirectTo: '/dashboard', pathMatch: 'prefix' },
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule { }
