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

@NgModule({
    declarations: [DashboardComponent, LoggingComponent, CloudConfigsComponent,
        CloudGraphComponent, CollectionsComponent, JavaPropsComponent, ThreadDumpComponent],
    imports: [
        CommonModule,
        FlexLayoutModule,
        MaterialModule,
        FontAwesomeModule,
        MomentModule,
        DialogsModule,
        ComponentsModule,
        NgMathPipesModule,
        NgxChartsModule,
        MatExpansionModule,
        MatTableModule,
        MatFormFieldModule
    ]
})
export class PagesModule { }
