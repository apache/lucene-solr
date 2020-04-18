import { JmxData } from './jmx-data';
import { JreData } from './jre-data';
import { MemoryData } from './memory-data';
import { JvmSpecData } from './jvm-spec-data';
import { VmData } from './vm-data';

export class JvmData {
    jmx: JmxData;
    jre: JreData;
    memory: MemoryData;
    name: string;
    processors: number;
    spec: JvmSpecData;
    version: string;
    vm: VmData;
}
