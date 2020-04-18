import { ThreadCountData } from './thread-count-data';
import { SolrResponseHeader } from '../solr-response-header';

export class ThreadDumpResponse {
    system: {
        threadCountData: ThreadCountData;
        responseHeader: SolrResponseHeader;
        threadDumpData: {
            thread: {
                id: number;
                name: string;
                state: string;
                cpuTime: string;
                userTime: string;
                stackTrace: string[];
            }[];
         };

    };
    
}
