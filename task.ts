import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, fetch, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const InputSchema = Type.Object({
    API_URL: Type.String({
        description: 'The URL of the API to fetch data from (Typically ends with /Production)'
    }),
    API_Token: Type.String({
        description: 'The API token for authentication'
    }),
    DataType: Type.String({
        enum: ['incidents', 'units'],
        default: 'incidents',
        description: 'The type of data to fetch'
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
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                const task = new Task();

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

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        const errors: Error[] = [];
        const features: Static<typeof Feature.InputFeature>[] = [];

        if (env.DataType === 'incidents') {
            for (const agency of env.Agencies) {
                try {
                    const res = await fetch(`${env.API_URL}/v1/GetActiveIncidentsByJurisdiction`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'x-api-key': env.API_Token,
                        },
                        body: JSON.stringify({
                            JurisdictionCodes: [ agency.id]
                        })
                    });

                    if (!res.ok) {
                        throw new Error(`Failed to fetch incidents: ${res.status} ${res.statusText}`);
                    }

                    const incidents = await res.typed(Type.Object({
                        Success: Type.Boolean(),
                        Error: Type.Optional(Type.String()),
                        Incidents: Type.Union([
                            Type.Null(),
                            Type.Array(OutputIncident)
                        ])
                    }), {
                        verbose: env.DEBUG || !!process.env.DEBUG
                    })

                    if (!incidents.Success) {
                        console.error('Error', incidents);
                        errors.push(new Error(`API Error for agency ${agency.name} (${agency.id}): ${incidents.Error || 'Unknown error'}`));
                    }

                    for (const incident of incidents.Incidents || []) {
                        if (incident.LocationInformation.Latitude && incident.LocationInformation.Longitude) {
                            console.error(incident.LocationInformation);
                            const feature: Static<typeof Feature.InputFeature> = {
                                id: String(incident.IncidentId),
                                type: 'Feature',
                                properties: {
                                    type: 'a-f-G',
                                    how: 'm-g',
                                    callsign: incident.IncidentType.Incident_Type || 'Unknown Incident',
                                    remarks: ''
                                },
                                geometry: {
                                    type: 'Point',
                                    coordinates: [
                                        incident.LocationInformation.Longitude,
                                        incident.LocationInformation.Latitude,
                                    ]
                                }
                            };

                            features.push(feature);
                        }
                    }
                } catch (err) {
                    errors.push(new Error(`Error processing Incident: ${agency.name} (${agency.id}): ${(err as Error).message}`));
                }
            }
        } else if (env.DataType === 'units') {
            for (const agency of env.Agencies) {
                try {
                    const res = await fetch(`${env.API_URL}/v1/GetActiveUnitsByJurisdiction`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'x-api-key': env.API_Token,
                        },
                        body: JSON.stringify({
                            JurisdictionCodes: [ agency.id ]
                        })
                    });

                    const units = await res.typed(Type.Object({
                        Success: Type.Boolean(),
                        Error: Type.Optional(Type.String()),
                        Units: Type.Array(OutputUnit)
                    }), {
                        verbose: env.DEBUG || !!process.env.DEBUG
                    })

                    if (!units.Success) {
                        console.error('Error', units);
                        errors.push(new Error(`API Error for agency ${agency.name} (${agency.id}): ${units.Error || 'Unknown error'}`));
                    }

                    for (const unit of units.Units) {
                        const feature: Static<typeof Feature.InputFeature> = {
                            id: String(unit.UnitID),
                            type: 'Feature',
                            properties: {
                                type: 'a-f-G',
                                how: 'm-g',
                                callsign: unit.UnitName,
                                remarks: unit.StatusName
                            },
                            geometry: {
                                type: 'Point',
                                coordinates: [
                                    unit.Longitude,
                                    unit.Latitude,
                                ]
                            }
                        };

                        features.push(feature);
                    }
                } catch (err) {
                    errors.push(new Error(`Error processing Unit: ${agency.name} (${agency.id}): ${(err as Error).message}`));
                }
            }
        } else {
            throw new Error(`Unsupported DataType: ${env.DataType}`);
        }


        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);

        if (errors.length > 0) {
            throw new Error(`Encountered ${errors.length} errors:\n` + errors.map(e => e.message).join('\n'));
        }
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

