import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SolrVersionDialogComponent } from './solr-version-dialog/solr-version-dialog.component';
import { MaterialModule } from '../material/material.module';
import { InfoDialogComponent } from './info-dialog/info-dialog.component';

@NgModule({
  declarations: [
    SolrVersionDialogComponent,
    InfoDialogComponent
  ],
  entryComponents: [
    SolrVersionDialogComponent,
    InfoDialogComponent
  ],
  imports: [
    CommonModule,
    MaterialModule
  ],
  exports: [
    SolrVersionDialogComponent,
    InfoDialogComponent
  ]
})
export class DialogsModule { }
