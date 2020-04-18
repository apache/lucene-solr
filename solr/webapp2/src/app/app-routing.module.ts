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
