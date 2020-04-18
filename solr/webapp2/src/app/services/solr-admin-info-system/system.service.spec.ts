import { TestBed } from '@angular/core/testing';

import { SystemService } from './system.service';

describe('SystemService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SystemService = TestBed.get(SystemService);
    expect(service).toBeTruthy();
  });
});
