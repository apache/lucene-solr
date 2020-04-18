import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CloudConfigsComponent } from './cloud-configs.component';

describe('CloudConfigsComponent', () => {
  let component: CloudConfigsComponent;
  let fixture: ComponentFixture<CloudConfigsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CloudConfigsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CloudConfigsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
