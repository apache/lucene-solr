import { TestBed } from '@angular/core/testing';

import { SolrService } from './solr.service';

describe('SolrService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SolrService = TestBed.get(SolrService);
    expect(service).toBeTruthy();
  });
});
