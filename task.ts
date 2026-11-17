import { Static, Type, TSchema } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
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
    ShortcutId: Type.Union([Type.Null(), Type.Number()]),
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
    Reopened: Type.Boolean(),
    FirstThreeComments: Type.Optional(Type.Array(Type.String())),
    PagingNeeded: Type.Optional(Type.String()),
    Units: Type.Optional(Type.Array(Type.Object({
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
        CurrentLocation: Type.String(),
        Speed: Type.Union([Type.Null(), Type.Number()]),
        DestinationLatitude: Type.Union([Type.Null(), Type.Number()]),
        DestinationLongitude: Type.Union([Type.Null(), Type.Number()]),
        Heading: Type.Union([Type.Null(), Type.String()]),
        CADIncident: Type.Union([Type.Null(), Type.String()])
    })))
})

const Nullable = <T extends TSchema>(type: T) => Type.Optional(Type.Union([Type.Null(), type]));

const WebhookUnit = Type.Object({
    UnitName: Nullable(Type.String()),
    UnitID: Nullable(Type.Union([Type.Number(), Type.String()])),
    VehicleName: Nullable(Type.String()),
    StatusName: Nullable(Type.String()),
    Latitude: Nullable(Type.Number()),
    Longitude: Nullable(Type.Number()),
    IncidentID: Nullable(Type.Number()),
    Agency: Nullable(Type.String()),
    Jurisdiction: Nullable(Type.String()),
    CurrentLocation: Nullable(Type.String()),
    Speed: Nullable(Type.Union([Type.Number(), Type.String()])),
    Heading: Nullable(Type.Union([Type.Number(), Type.String()])),
    Personel: Nullable(Type.Array(Type.String()))
});

const WebhookIncident = Type.Object({
    IncidentId: Nullable(Type.Union([Type.Number(), Type.String()])),
    ShortcutId: Nullable(Type.Union([Type.Number(), Type.String()])),
    Master_Incident_Number: Nullable(Type.String()),
    Response_Date: Nullable(Type.String()),
    IncidentType: Nullable(Type.Object({
        Incident_Type: Nullable(Type.String()),
        Problem: Nullable(Type.String()),
        PriorityDescription: Nullable(Type.String()),
        Determinant: Nullable(Type.String())
    })),
    IncidentHierarchy: Nullable(Type.Object({
        Agency_Type: Nullable(Type.String()),
        Division: Nullable(Type.String()),
        Battalion: Nullable(Type.String())
    })),
    CallerInformation: Nullable(Type.Object({
        Caller_Name: Nullable(Type.String()),
        Call_Back_Phone: Nullable(Type.String())
    })),
    LocationInformation: Nullable(Type.Object({
        Address: Nullable(Type.String()),
        Apartment: Nullable(Type.String()),
        City: Nullable(Type.String()),
        Location_Name: Nullable(Type.String()),
        Cross_Street: Nullable(Type.String()),
        Latitude: Nullable(Type.Number()),
        Longitude: Nullable(Type.Number())
    })),
    Call_Disposition: Nullable(Type.String()),
    WhichQueue: Nullable(Type.String())
});

type InputFeature = Static<typeof Feature.InputFeature>;

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function isFeatureList(value: object): value is { total: number; items: Array<InputFeature> } {
    return 'total' in value && typeof value.total === 'number'
        && 'items' in value && Array.isArray(value.items)
        && value.items.every((item: unknown) => isObject(item) && isObject(item.properties));
}

function unitFeature(unit: Static<typeof WebhookUnit>): InputFeature | undefined {
    if (typeof unit.Latitude !== 'number' || typeof unit.Longitude !== 'number') return undefined;

    const remarks = [
        unit.StatusName ? `Status: ${unit.StatusName}` : null,
        unit.Agency ? `Agency: ${unit.Agency}` : null,
        unit.Jurisdiction ? `Jurisdiction: ${unit.Jurisdiction}` : null,
        unit.CurrentLocation ? `Location: ${unit.CurrentLocation}` : null,
        (unit.IncidentID ?? 0) !== 0 ? `Incident ID: ${unit.IncidentID}` : null,
        unit.Personel && unit.Personel.length > 0 ? `Personnel: ${unit.Personel.join(', ')}` : null
    ].filter(Boolean).join('\n');

    const now = new Date();
    const speed = unit.Speed ?? undefined;
    const course = unit.Heading ?? undefined;

    return {
        id: `unit-${unit.UnitID}`,
        type: 'Feature',
        properties: {
            callsign: (unit.UnitName || unit.VehicleName) ?? undefined,
            type: 'a-f-G-E-V',
            how: 'm-g',
            time: now.toISOString(),
            start: now.toISOString(),
            stale: new Date(now.getTime() + 20 * 60 * 1000).toISOString(),
            remarks: remarks || undefined,
            track: (speed !== undefined || course !== undefined) ? {
                speed: speed !== undefined ? String(speed) : undefined,
                course: course !== undefined ? String(course) : undefined
            } : undefined,
            metadata: unit
        },
        geometry: {
            type: 'Point',
            coordinates: [unit.Longitude, unit.Latitude]
        }
    };
}

function incidentFeature(incident: Static<typeof WebhookIncident>): InputFeature | undefined {
    const location = incident.LocationInformation;

    if (!location || typeof location.Latitude !== 'number' || typeof location.Longitude !== 'number') return undefined;

    const address = [
        location.Address,
        location.Apartment,
        location.City
    ].filter(Boolean).join(', ');

    const remarks = [
        address ? `Address: ${address}` : null,
        location.Location_Name ? `Location: ${location.Location_Name}` : null,
        location.Cross_Street ? `Cross Street: ${location.Cross_Street}` : null,
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

    const now = new Date();
    const start = new Date(incident.Response_Date || Date.now());
    const shortcut = incident.ShortcutId ?? undefined;

    return {
        id: `incident-${incident.IncidentId}`,
        type: 'Feature',
        properties: {
            callsign: incident.IncidentType?.Problem
                || incident.IncidentType?.Incident_Type
                || (shortcut !== undefined ? String(shortcut) : null)
                || incident.Master_Incident_Number
                || `Incident ${incident.IncidentId}`,
            type: 'a-f-G-U-i',
            how: 'h-g-i-g-o',
            time: start.toISOString(),
            start: start.toISOString(),
            stale: new Date(now.getTime() + 20 * 60 * 1000).toISOString(),
            remarks: remarks || undefined,
            metadata: incident
        },
        geometry: {
            type: 'Point',
            coordinates: [location.Longitude, location.Latitude]
        }
    };
}

export default class Task extends ETL {
    static name = 'etl-jeffcom'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule, InvocationType.Webhook ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow !== DataFlowType.Incoming) return Type.Object({});

        if (type === SchemaType.Input) return InputSchema;

        const env = await this.env(InputSchema);

        if (env.DataType === 'incidents') return OutputIncident;
        if (env.DataType === 'units') return OutputUnit;

        throw new Error(`Unsupported DataType: ${env.DataType}`);
    }

    static async webhooks(
        schema: Schema,
        task: ETL
    ): Promise<void> {
        const env = await task.env(InputSchema);

        await schema.post('/:webhookid', {
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
        }, (req, res, next) => {
            if (!req.headers.authorization || req.headers.authorization.split(' ')[1] !== env.APIKey) {
                res.status(401).json({
                    status: 401,
                    message: 'Unauthorized'
                });
                return;
            }

            if (env.DEBUG) {
                console.error(`DEBUG Webhook: ${req.params.webhookid} - ${JSON.stringify(req.body, null, 4)}`);
            }

            Task.receive(task, env.DataType, req.body)
                .then(() => res.json({
                    status: 200,
                    message: 'Received'
                }))
                // oxlint-disable-next-line promise/no-callback-in-promise -- forwards to the Express error handler
                .catch(next);
        });
    }

    static async receive(
        task: ETL,
        type: string,
        body: unknown
    ): Promise<void> {
        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        if (type === 'units') {
            if (!Value.Check(WebhookUnit, body)) throw new Error('Invalid Unit body');

            const feat = unitFeature(body);
            if (feat) fc.features.push(feat);
        } else if (type === 'incidents') {
            if (!Value.Check(WebhookIncident, body)) throw new Error('Invalid Incident body');

            const feat = incidentFeature(body);
            if (feat) fc.features.push(feat);
        }

        await task.submit(fc);
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();
        const now = new Date();

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

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

            const res = await this.fetch(url);
            if (!isFeatureList(res)) throw new Error('Unexpected Feature list response');

            total = res.total;

            for (const feat of res.items) {
                const { stale } = feat.properties;
                const expiry = (typeof stale === 'string' || typeof stale === 'number') && stale !== 0 ? new Date(stale) : null;
                if (expiry && !Number.isNaN(expiry.getTime()) && expiry < now) continue;

                feat.properties.archived = false;
                fc.features.push(feat);
            }

            if (res.items.length < limit) break;
            page++;
        }

        await this.submit(fc, {
            archive: false
        });
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}, context?: object): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url, {
        logging: {
            event: Boolean(process.env.DEBUG),
            webhooks: true
        }
    }), event, context);
}
