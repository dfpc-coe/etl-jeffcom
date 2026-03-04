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
    RebroadcastTimeout: Type.Number({
        default: 60,
        description: 'Number of minutes for which features should be rebroadcast'
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
            if (!req.headers.authorization || req.headers.authorization.split(' ')[1] !== env.APIKey) {
                return res.status(401).json({
                    status: 401,
                    message: 'Unauthorized'
                });
            }

            if (env.DEBUG) {
                console.error(`DEBUG Webhook: ${req.params.webhookid} - ${JSON.stringify(req.body, null, 4)}`);
            }

            const fc: Static<typeof Feature.InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: []
            };

            if (env.DataType === 'units') {
                const unit = req.body as Static<typeof OutputUnit>;

                if (unit.Latitude != null && unit.Longitude != null) {
                    const remarks = [
                        unit.StatusName ? `Status: ${unit.StatusName}` : null,
                        unit.Agency ? `Agency: ${unit.Agency}` : null,
                        unit.Jurisdiction ? `Jurisdiction: ${unit.Jurisdiction}` : null,
                        unit.CurrentLocation ? `Location: ${unit.CurrentLocation}` : null,
                        unit.IncidentID ? `Incident ID: ${unit.IncidentID}` : null,
                        unit.Personel?.length ? `Personnel: ${unit.Personel.join(', ')}` : null
                    ].filter(Boolean).join('\n');

                    fc.features.push({
                        id: `unit-${unit.UnitID}`,
                        type: 'Feature',
                        properties: {
                            callsign: unit.UnitName || unit.VehicleName,
                            type: 'a-f-G-E-V',
                            how: 'm-g',
                            time: new Date().toISOString(),
                            start: new Date().toISOString(),
                            stale: 600,
                            remarks: remarks || undefined,
                            track: (unit.Speed != null || unit.Heading != null) ? {
                                speed: unit.Speed != null ? String(unit.Speed) : undefined,
                                course: unit.Heading != null ? String(unit.Heading) : undefined
                            } : undefined,
                            metadata: { original: unit }
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: [unit.Longitude, unit.Latitude]
                        }
                    });
                }
            } else if (env.DataType === 'incidents') {
                const incident = req.body as Static<typeof OutputIncident>;

                const lat = incident.LocationInformation?.Latitude;
                const lon = incident.LocationInformation?.Longitude;

                if (lat != null && lon != null) {
                    const address = [
                        incident.LocationInformation.Address,
                        incident.LocationInformation.Apartment,
                        incident.LocationInformation.City
                    ].filter(Boolean).join(', ');

                    const remarks = [
                        address ? `Address: ${address}` : null,
                        incident.LocationInformation.Location_Name ? `Location: ${incident.LocationInformation.Location_Name}` : null,
                        incident.LocationInformation.Cross_Street ? `Cross Street: ${incident.LocationInformation.Cross_Street}` : null,
                        incident.IncidentType?.Incident_Type ? `Type: ${incident.IncidentType.Incident_Type}` : null,
                        incident.IncidentType?.Problem ? `Problem: ${incident.IncidentType.Problem}` : null,
                        incident.IncidentType?.PriorityDescription ? `Priority: ${incident.IncidentType.PriorityDescription}` : null,
                        incident.IncidentType?.Determinant ? `Determinant: ${incident.IncidentType.Determinant}` : null,
                        incident.IncidentHierarchy?.Agency_Type ? `Agency: ${incident.IncidentHierarchy.Agency_Type}` : null,
                        incident.IncidentHierarchy?.Division ? `Division: ${incident.IncidentHierarchy.Division}` : null,
                        incident.IncidentHierarchy?.Battalion ? `Battalion: ${incident.IncidentHierarchy.Battalion}` : null,
                        incident.CallerInformation?.Caller_Name ? `Caller: ${incident.CallerInformation.Caller_Name}` : null,
                        incident.CallerInformation?.Call_Back_Phone ? `Phone: ${incident.CallerInformation.Call_Back_Phone}` : null,
                        incident.Call_Disposition ? `Disposition: ${incident.Call_Disposition}` : null,
                        incident.WhichQueue ? `Queue: ${incident.WhichQueue}` : null,
                        incident.Master_Incident_Number ? `Master Incident: ${incident.Master_Incident_Number}` : null
                    ].filter(Boolean).join('\n');

                    fc.features.push({
                        id: `incident-${incident.IncidentId}`,
                        type: 'Feature',
                        properties: {
                            callsign: incident.IncidentType?.Problem
                                || incident.IncidentType?.Incident_Type
                                || incident.ShortcutId
                                || incident.Master_Incident_Number
                                || `Incident ${incident.IncidentId}`,
                            type: 'a-f-G-U-i',
                            how: 'h-g-i-g-o',
                            time: incident.Response_Date || new Date().toISOString(),
                            start: incident.Response_Date || new Date().toISOString(),
                            stale: 3600,
                            remarks: remarks || undefined,
                            metadata: { original: incident }
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: [lon, lat]
                        }
                    });
                }
            }

            await task.submit(fc);

            res.json({
                status: 200,
                message: 'Received'
            });
        });
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);
        const layer = await this.fetchLayer();

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        const cutoff = new Date(Date.now() - env.RebroadcastTimeout * 60 * 1000);

        const limit = 100;
        let page = 0;
        let total = Infinity;

        while (fc.features.length < total) {
            const url = new URL(`/api/connection/${layer.connection}/feature`, this.etl.api);
            url.searchParams.set('layer', String(layer.id));
            url.searchParams.set('format', 'geojson');
            url.searchParams.set('download', 'false');
            url.searchParams.set('limit', String(limit));
            url.searchParams.set('page', String(page));
            url.searchParams.set('sort', 'id');
            url.searchParams.set('order', 'asc');
            url.searchParams.set('filter', '');

            const res = await this.fetch(url) as { total: number; items: Static<typeof Feature.InputFeatureCollection>['features'] };

            total = res.total;

            for (const feat of res.items) {
                const time = feat.properties?.start ? new Date(feat.properties.start as string) : null;
                if (!time || time >= cutoff) {
                    fc.features.push(feat);
                }
            }

            if (res.items.length < limit) break;
            page++;
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
