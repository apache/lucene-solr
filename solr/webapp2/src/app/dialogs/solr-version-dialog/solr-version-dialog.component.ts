import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
  selector: 'app-solr-version-dialog',
  templateUrl: './solr-version-dialog.component.html',
  styleUrls: ['./solr-version-dialog.component.scss']
})
export class SolrVersionDialogComponent implements OnInit {

  constructor(public dialogRef: MatDialogRef<SolrVersionDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any) { }

  ngOnInit() {
  }

}
