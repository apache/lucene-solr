import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MasterComponent } from './master/master.component';
import { MaterialModule } from '../material/material.module';
import { RouterModule } from '@angular/router';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';

@NgModule({
  declarations: [MasterComponent],
  imports: [
    CommonModule,
    RouterModule,
    MaterialModule,
    FontAwesomeModule
  ],
  exports: [
      MasterComponent
  ]
})
export class AppLayoutModule { }
