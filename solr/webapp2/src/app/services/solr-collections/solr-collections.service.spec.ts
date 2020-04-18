import { TestBed } from '@angular/core/testing';

import { SolrCollectionsService } from './solr-collections.service';

describe('SolrCollectionsService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SolrCollectionsService = TestBed.get(SolrCollectionsService);
    expect(service).toBeTruthy();
  });
});
