import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const InputSchema = Type.Object({
    API_URL: Type.String({
        description: 'The URL of the API to fetch data from'
    }),
    API_Token: Type.String({
        description: 'The API token for authentication'
    }),
    Agencies: Type.Array(Type.Object({
        id: Type.String({ description: 'The agency ID' }),
        name: Type.String({ description: 'The agency name' })
    })),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({})

export default class Task extends ETL {
    static name = 'etl-jeffcom'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        for (const agency of env.Agencies) {
            console.log(`Configured agency: ${agency.name} (ID: ${agency.id})`);
        }

        const features: Static<typeof Feature.InputFeature>[] = [];

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

