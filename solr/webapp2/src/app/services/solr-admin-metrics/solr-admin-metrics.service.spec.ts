import { TestBed } from '@angular/core/testing';

import { SolrAdminMetricsService } from './solr-admin-metrics.service';

describe('SolrAdminMetricsService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SolrAdminMetricsService = TestBed.get(SolrAdminMetricsService);
    expect(service).toBeTruthy();
  });
});
