import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SolrVersionDialogComponent } from './solr-version-dialog.component';

describe('SolrVersionDialogComponent', () => {
  let component: SolrVersionDialogComponent;
  let fixture: ComponentFixture<SolrVersionDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SolrVersionDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SolrVersionDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
