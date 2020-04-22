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
import { CommonModule } from '@angular/common';
import { DashboardComponent } from './dashboard/dashboard.component';
import { LoggingComponent } from './logging/logging.component';
import { CloudConfigsComponent } from './cloud-configs/cloud-configs.component';
import { CloudGraphComponent } from './cloud-graph/cloud-graph.component';
import { CollectionsComponent } from './collections/collections.component';
import { JavaPropsComponent } from './java-props/java-props.component';
import { ThreadDumpComponent } from './thread-dump/thread-dump.component';
import { FlexLayoutModule } from '@angular/flex-layout';
import { MaterialModule } from '../material/material.module';
import { MomentModule } from 'ngx-moment';
import { DialogsModule } from '../dialogs/dialogs.module';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import { ComponentsModule } from '../components/components.module';
import { NgMathPipesModule } from 'angular-pipes';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import {MatExpansionModule} from '@angular/material/expansion';
import { NgxGraphModule } from '@swimlane/ngx-graph';
import { MatTableModule } from '@angular/material/table';
import { MatFormFieldModule } from '@angular/material/form-field';
import {FormsModule, ReactiveFormsModule} from "@angular/forms";

@NgModule({
    declarations: [DashboardComponent, LoggingComponent, CloudConfigsComponent,
        CloudGraphComponent, CollectionsComponent, JavaPropsComponent, ThreadDumpComponent],
  imports: [
    CommonModule,
    FlexLayoutModule,
    MaterialModule,
    FontAwesomeModule,
    FormsModule,
    MomentModule,
    DialogsModule,
    ComponentsModule,
    NgMathPipesModule,
    NgxChartsModule,
    NgxGraphModule,
    MatExpansionModule,
    MatTableModule,
    MatFormFieldModule,
    ReactiveFormsModule
  ]
})
export class PagesModule { }
