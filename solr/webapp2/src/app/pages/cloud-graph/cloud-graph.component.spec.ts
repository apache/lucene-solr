import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CloudGraphComponent } from './cloud-graph.component';

describe('CloudGraphComponent', () => {
  let component: CloudGraphComponent;
  let fixture: ComponentFixture<CloudGraphComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CloudGraphComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CloudGraphComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
