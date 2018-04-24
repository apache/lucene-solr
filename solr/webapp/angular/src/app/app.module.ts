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

import { AppRoutingModule } from './app-routing.module';
import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {HttpClientModule} from '@angular/common/http';
import { FormsModule }   from '@angular/forms';
import {TimeAgoPipe} from 'time-ago-pipe';
import { NgSelectModule } from '@ng-select/ng-select';
import {AppComponent} from './app.component';
import { CollectionsComponent } from './collections/collections.component';
import { SystemInfoService } from './systeminfo.service';
import { CollectionsService } from './collections.service';
import { CoresService } from './cores.service';
import { ZookeeperService } from './zookeeper.service';
import { SharedService } from './shared.service';
import { DashboardComponent } from './dashboard/dashboard.component';
import { CoresComponent } from './cores/cores.component';
import { MessagesComponent } from './messages/messages.component';
import { MenuComponent } from './menu/menu.component';
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

@NgModule({
  declarations: [
    AppComponent,
    TimeAgoPipe,
    MenuComponent,
    MessagesComponent,
    DashboardComponent,
    CollectionsComponent,
    CoresComponent,
    AnalysisComponent,
    DihComponent,
    DocumentsComponent,
    FilesComponent,
    QueryComponent,
    StreamComponent,
    SchemaComponent,
    PluginsComponent,
    ReplicationComponent,
    SegmentsComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    AppRoutingModule,
    FormsModule,
    NgSelectModule
  ],
  providers: [
    SharedService,
    SystemInfoService,
    CollectionsService,
    CoresService,
    ZookeeperService
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}
