import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import Schema from '@openaddresses/batch-schema';

const InputSchema = Type.Object({
    DataType: Type.String({
        enum: [
            'incidents',
            'units'
        ],
        description: 'Type of data to fetch'
    }),
    APIKey: Type.String({
        description: 'API Key for webhook permissions'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputUnit = Type.Object({
    UnitName: Type.String(),
    UnitID: Type.Number(),
    VehicleName: Type.String(),
    VehicleID: Type.Number(),
    StatusName: Type.String(),
    Latitude: Type.Number(),
    Longitude: Type.Number(),
    IncidentID: Type.Number(),
    Agency: Type.String(),
    Jurisdiction: Type.String(),
    JurisdictionCode: Type.String(),
    CurrentLocation: Type.String(),
    Speed: Type.Union([Type.Null(), Type.Number()]),
    DestinationLatitude: Type.Union([Type.Null(), Type.Number()]),
    DestinationLongitude: Type.Union([Type.Null(), Type.Number()]),
    Heading: Type.Union([Type.Null(), Type.String()]),
    Personel: Type.Array(Type.String())
});

const OutputIncident = Type.Object({
    IncidentId: Type.Number(),
    ShortcutId: Type.Union([Type.Null(), Type.String()]),
    Master_Incident_Number: Type.Union([Type.Null(), Type.String()]),
    CaseNumbers: Type.Array(Type.String()),
    Response_Date: Type.String(),
    IncidentType: Type.Object({
        Incident_Type: Type.Union([Type.Null(), Type.String()]),
        Problem: Type.Union([Type.Null(), Type.String()]),
        Priority: Type.Union([Type.Null(), Type.Number()]),
        PriorityDescription: Type.Union([Type.Null(), Type.String()]),
        Response_Plan: Type.Union([Type.Null(), Type.String()]),
        Determinant: Type.Union([Type.Null(), Type.String()]),
    }),
    IncidentHierarchy: Type.Object({
        Agency_Type: Type.String(),
        Jurisdiction: Type.String(),
        Division: Type.Union([Type.Null(), Type.String()]),
        Battalion: Type.Union([Type.Null(), Type.String()]),
        Response_Area: Type.Union([Type.Null(), Type.String()])
    }),
    CallerInformation: Type.Object({
        Caller_Name: Type.Union([Type.Null(), Type.String()]),
        Call_Back_Phone: Type.Union([Type.Null(), Type.String()]),
        MethodOfCallRcvd: Type.Union([Type.Null(), Type.String()]),
    }),
    LocationInformation: Type.Object({
        Address: Type.Union([Type.Null(), Type.String()]),
        Apartment: Type.Union([Type.Null(), Type.String()]),
        City: Type.Union([Type.Null(), Type.String()]),
        Location_Name: Type.Union([Type.Null(), Type.String()]),
        Cross_Street: Type.Union([Type.Null(), Type.String()]),
        Latitude: Type.Union([Type.Null(), Type.Number()]),
        Longitude: Type.Union([Type.Null(), Type.Number()]),
    }),
    IncidentTimes: Type.Object({
        Time_PhonePickUp: Type.Union([Type.Null(), Type.String()]),
        Time_FirstCallTakingKeystroke: Type.Union([Type.Null(), Type.String()]),
        Time_CallEnteredQueue: Type.Union([Type.Null(), Type.String()]),
        Time_CallTakingComplete: Type.Union([Type.Null(), Type.String()]),
        Time_CallClosed: Type.Union([Type.Null(), Type.String()]),
        Fixed_Time_CallEnteredQueue: Type.Union([Type.Null(), Type.String()]),
        Fixed_Time_CallClosed: Type.Union([Type.Null(), Type.String()]),
        Time_FirstUnitAssigned: Type.Union([Type.Null(), Type.String()]),
        Time_FirstUnitEnroute: Type.Union([Type.Null(), Type.String()]),
        Time_FirstUnitStaged: Type.Union([Type.Null(), Type.String()]),
        Time_FirstUnitArrived: Type.Union([Type.Null(), Type.String()])
    }),
    CallTaking_Performed_By: Type.Union([Type.Null(), Type.String()]),
    CallClosing_Performed_By: Type.Union([Type.Null(), Type.String()]),
    Call_Disposition: Type.Union([Type.Null(), Type.String()]),
    Cancel_Reason: Type.Union([Type.Null(), Type.String()]),
    WhichQueue: Type.String(),
    Call_Is_Active: Type.Boolean(),
    RequestToCancel: Type.Boolean(),
    Stacked: Type.Boolean(),
    Reopened: Type.Boolean()
})

export default class Task extends ETL {
    static name = 'etl-jeffcom'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule, InvocationType.Webhook ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                const env = await this.env(InputSchema);

                if (env.DataType === 'incidents') {
                    return OutputIncident;
                } else if (env.DataType === 'units') {
                    return OutputUnit;
                } else {
                    throw new Error(`Unsupported DataType: ${env.DataType}`);
                }
            }
        } else {
            return Type.Object({});
        }
    }

    static async webhooks(
        schema: Schema,
        task: Task
    ): Promise<void> {
        const env = await task.env(InputSchema);

        schema.post('/:webhookid', {
            name: 'Incoming Webhook',
            group: 'Default',
            description: 'JeffCom Data Event',
            params: Type.Object({
                webhookid: Type.String()
            }),
            body: Type.Any(),
            res: Type.Object({
                status: Type.Number(),
                message: Type.String()
            })
        }, async (req: any, res: any) => {
            if (env.DEBUG) {
                console.error(`DEBUG Webhook: ${req.params.webhookid} - ${JSON.stringify(req.body, null, 4)}`);
            }

            res.json({
                status: 200,
                message: 'Received'
            });
        });
    }

    async control(): Promise<void> {
        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        }

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}, context?: object) {
    return await internal(await Task.init(import.meta.url, {
        logging: {
            event: process.env.DEBUG ? true : false,
            webhooks: true
        }
    }), event, context);
}
