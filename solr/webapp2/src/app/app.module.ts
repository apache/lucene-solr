import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { AppLayoutModule } from './app-layout/app-layout.module';
import { PagesModule } from './pages/pages.module';
import { IconsModule } from './icons/icons.module';
import { HttpClientModule } from '@angular/common/http';
import { MAT_DIALOG_DEFAULT_OPTIONS} from '@angular/material/dialog';
import {MatExpansionModule} from '@angular/material/expansion';
import {MatTableModule} from '@angular/material/table';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import { MatFormFieldModule } from '@angular/material/form-field';
import {Sort, MatSortModule} from '@angular/material/sort';
import { MatTabsModule } from '@angular/material/tabs';
import { MatDividerModule } from '@angular/material/divider';
import {MatTreeModule} from '@angular/material/tree';
import {NestedTreeControl} from '@angular/cdk/tree';
@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    HttpClientModule,
    FontAwesomeModule,
    AppLayoutModule,
    PagesModule,
    IconsModule,
    MatExpansionModule,
    MatTableModule,
    MatFormFieldModule,
    MatSortModule,
    MatTabsModule,
    MatDividerModule,
    MatTreeModule
  ],
  providers: [
    {provide: MAT_DIALOG_DEFAULT_OPTIONS, useValue: {hasBackdrop: true},
  }
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
