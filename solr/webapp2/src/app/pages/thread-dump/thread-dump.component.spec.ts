import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ThreadDumpComponent } from './thread-dump.component';

describe('ThreadDumpComponent', () => {
  let component: ThreadDumpComponent;
  let fixture: ComponentFixture<ThreadDumpComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ThreadDumpComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ThreadDumpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
