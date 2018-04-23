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

const routes: Routes = [
  { path: 'dashboard', component: DashboardComponent },
  { path: 'collections/:name', component: CollectionsComponent },
  { path: 'collections/:name/overview', component: CollectionsComponent },
  { path: 'collections/:name/analysis', component: AnalysisComponent },
  { path: 'collections/:name/dataimport', component: DihComponent },
  { path: 'collections/:name/documents', component: DocumentsComponent },
  { path: 'collections/:name/files', component: FilesComponent },
  { path: 'collections/:name/query', component: QueryComponent },
  { path: 'collections/:name/stream', component: StreamComponent },
  { path: 'collections/:name/schema', component: SchemaComponent },
  { path: 'collections', component: CollectionsComponent },
  { path: 'cores', component: CoresComponent },
  { path: '', redirectTo: '/dashboard', pathMatch: 'prefix' },
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule { }
