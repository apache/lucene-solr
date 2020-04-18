import { Component, OnInit } from '@angular/core';
import { JavaPropsService } from '../../services/solr-java-props/java-props.service';

@Component({
  selector: 'app-java-props',
  templateUrl: './java-props.component.html',
  styleUrls: ['./java-props.component.scss']
})
export class JavaPropsComponent implements OnInit {
  
  systemProperties: any;

  constructor(private javaPropsService: JavaPropsService) { }

  ngOnInit() {
    this.javaPropsService.getData().subscribe(
      response => {
          this.systemProperties = response["system.properties"];
      },
      err => {
          console.error(err);
      }
    );
  }

}
