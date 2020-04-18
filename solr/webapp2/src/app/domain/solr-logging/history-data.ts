export class HistoryData {
    numFound: number;
    start: number;
    docs: {
        time: string,
        level: string, 
        logger?: string, 
        messaage: string, 
        node_name?: string, 
        myid?: string, 
        core?: string
    }[];
}