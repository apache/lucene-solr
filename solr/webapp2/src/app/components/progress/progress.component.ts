import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-progress',
  templateUrl: './progress.component.html',
  styleUrls: ['./progress.component.scss']
})
export class ProgressComponent implements OnInit {
    @Input()
    label: string;
    @Input()
    value: string;
    @Input()
    min: string;
    @Input()
    max: string;
    @Input()
    mode = 'determinite';

  constructor() { }

  ngOnInit() {
  }

}
