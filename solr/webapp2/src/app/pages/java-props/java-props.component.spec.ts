import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { JavaPropsComponent } from './java-props.component';

describe('JavaPropsComponent', () => {
  let component: JavaPropsComponent;
  let fixture: ComponentFixture<JavaPropsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ JavaPropsComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(JavaPropsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
