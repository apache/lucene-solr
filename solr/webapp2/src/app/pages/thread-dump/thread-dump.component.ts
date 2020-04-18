import { Component, OnInit } from '@angular/core';
import { ThreadDumpService } from '../../services/solr-thread-dump/thread-dump.service';
import { ThreadDumpResponse } from 'src/app/domain/solr-thread-dump/thread-dump-response';
import {FlatTreeControl} from '@angular/cdk/tree';
// import {MatExpansionModule} from '@angular/material/expansion';
@Component({
  selector: 'thread-dump-component',
  templateUrl: './thread-dump.component.html',
  styleUrls: ['./thread-dump.component.scss']
})
export class ThreadDumpComponent implements OnInit {
panelOpenState = false;
threadDumpData;

  constructor(private threadDumpService: ThreadDumpService) { }

  ngOnInit() {
    this.threadDumpService.getData().subscribe(
      response => {
        this.threadDumpData = response["system"]["threadDump"];
        this.processThreadDump(this.threadDumpData);
      },
      err => {
          console.error(err);
      }
    );
  }
  processThreadDump(threadDumpData: any): any{
    this.threadDumpData = threadDumpData.filter(function(element, index){
      return index % 2 == 1
    })
  }
}
