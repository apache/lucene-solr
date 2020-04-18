import { Component, OnInit } from '@angular/core';
import { SolrCollectionsService } from '../../services/solr-collections/solr-collections.service';

@Component({
  selector: 'app-collections',
  templateUrl: './collections.component.html',
  styleUrls: ['./collections.component.scss']
})
export class CollectionsComponent implements OnInit {
    collectionNames;

  constructor(private collectionsService: SolrCollectionsService) { }

  ngOnInit() {
      this.collectionsService.get().subscribe(response => {
        this.collectionNames = response.collections;
      });
  }

}
