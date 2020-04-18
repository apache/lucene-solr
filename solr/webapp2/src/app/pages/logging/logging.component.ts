import { Component, AfterViewInit } from '@angular/core';
// import * as table from '@angular/material/table';
import { Sort, MatSortModule } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { LoggingService } from '../../services/solr-logging/logging.service';

export interface Docs {
  time: string;
  level: string;
  message: string;
  logger?: string;
  node_name?: string;
  myid?: string;
  core?: string;
}
@Component({
  selector: 'app-logging',
  templateUrl: './logging.component.html',
  styleUrls: ['./logging.component.scss']
})

export class LoggingComponent implements AfterViewInit {
  historyArray: Docs[];
  dataSource;
  sortedData: Docs[];
  history; 

  constructor(private loggingService: LoggingService, public sort: MatSortModule) {  }

  ngAfterViewInit() {
    this.loggingService.getData().subscribe(
      response => {
          this.history = response["history"];
          this.processHistoryData(this.history);
          this.sortedData = this.historyArray.slice();
      },
      err => {
          console.error(err);
      }
    );
  }
  processHistoryData(history: Docs) {
    this.historyArray = history["docs"];
    return this.historyArray;
  }
  sortData(sort: Sort) {
    const data = this.sortedData.slice();
    if (!sort.active || sort.direction === '') {
      this.sortedData = data;
      return;
    }

    this.sortedData = data.sort((a, b) => {
      const isAsc = sort.direction === 'asc';
      switch (sort.active) {
        case 'level': return compare(a.level, b.level, isAsc);
        case 'time': return compare(a.time, b.time, isAsc);
        case 'message': return compare(a.message, b.message, isAsc);
        default: return 0;
      }
    });
  }
}
function compare(a: number | string, b: number | string, isAsc: boolean) {
  return (a < b ? -1 : 1) * (isAsc ? 1 : -1);
}
