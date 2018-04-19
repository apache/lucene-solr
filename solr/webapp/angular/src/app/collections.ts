export class ResponseHeader {
    status: number;
    QTime: number;
}

export class ListCollections {
    responseHeader: ResponseHeader;
    collections: string[];
}
﻿