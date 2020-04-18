import { JvmData } from './jvm-data';
import { LuceneData } from './lucene-data';
import { SolrResponseHeader } from '../solr-response-header';
import { SystemData } from './system-data';

export class SolrSystemResponse {
    jvm: JvmData;
    lucene: LuceneData;
    mode: string;
    node: string;
    responseHeader: SolrResponseHeader;
    solr_home: string;
    system: SystemData;
    zkHost: string;
}
