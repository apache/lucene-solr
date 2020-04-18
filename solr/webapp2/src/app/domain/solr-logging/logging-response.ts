import { InfoData } from './info-data';
import { HistoryData } from './history-data';
import { SolrResponseHeader } from '../solr-response-header';

export class LoggingResponse {
    info: InfoData;
    history: HistoryData;
    responseHeader: SolrResponseHeader;
    watcher: string;
}
