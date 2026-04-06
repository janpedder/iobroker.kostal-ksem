'use strict';

/**
 * Minimaler Protobuf-Decoder ohne .proto-Schema.
 * Dekodiert wire-types 0 (varint), 1 (64-bit), 2 (length-delimited), 5 (32-bit).
 */

function decodeVarint(buf, pos) {
    let result = 0n;
    let shift = 0n;
    while (pos < buf.length) {
        const b = buf[pos++];
        result |= BigInt(b & 0x7f) << shift;
        if (!(b & 0x80)) break;
        shift += 7n;
    }
    return { value: result, pos };
}

function decodeZigzag64(n) {
    // zigzag: (n >>> 1) ^ -(n & 1)
    return (n >> 1n) ^ -(n & 1n);
}

/**
 * Parst einen Protobuf-Buffer und gibt ein Map<fieldNumber, Array<value>> zurück.
 * Werte sind: { type: 'varint'|'bytes'|'float'|'double', raw, signed? }
 */
function parseProto(buf) {
    const fields = new Map();
    let pos = 0;

    const add = (fn, entry) => {
        if (!fields.has(fn)) fields.set(fn, []);
        fields.get(fn).push(entry);
    };

    while (pos < buf.length) {
        let tag;
        try {
            const r = decodeVarint(buf, pos);
            tag = r.value; pos = r.pos;
        } catch { break; }

        const fn  = Number(tag >> 3n);
        const wt  = Number(tag & 7n);

        try {
            if (wt === 0) {
                const r = decodeVarint(buf, pos); pos = r.pos;
                add(fn, { type: 'varint', raw: r.value, signed: decodeZigzag64(r.value) });
            } else if (wt === 1) {
                const lo = buf.readUInt32LE(pos);
                const hi = buf.readUInt32LE(pos + 4);
                pos += 8;
                const val = BigInt(lo) | (BigInt(hi) << 32n);
                add(fn, { type: 'int64', raw: val, signed: BigInt.asIntN(64, val) });
            } else if (wt === 2) {
                const r = decodeVarint(buf, pos); pos = r.pos;
                const len = Number(r.value);
                const bytes = buf.slice(pos, pos + len); pos += len;
                // Versuch als UTF-8 String
                try {
                    const str = bytes.toString('utf8');
                    // Nur wenn alle Zeichen druckbar sind
                    if ([...str].every(c => c.charCodeAt(0) >= 32 || '\n\r\t'.includes(c))) {
                        add(fn, { type: 'string', value: str });
                    } else {
                        add(fn, { type: 'bytes', value: bytes });
                    }
                } catch {
                    add(fn, { type: 'bytes', value: bytes });
                }
            } else if (wt === 5) {
                const val = buf.readFloatLE(pos); pos += 4;
                add(fn, { type: 'float', value: val });
            } else {
                break; // unbekannter wire type
            }
        } catch { break; }
    }
    return fields;
}

/**
 * Dekodiert eine KSEM Smart-Meter Protobuf-Nachricht.
 * Gibt ein Array von { id: BigInt, rawValue: BigInt } zurück.
 */
function decodeSmartMeterMessage(buf) {
    const outer = parseProto(buf);
    const datapoints = [];

    // Äussere Struktur: field 1 = Hauptblock
    const f1 = outer.get(1);
    if (!f1 || f1[0].type !== 'bytes') return datapoints;

    const main = parseProto(f1[0].value);
    // field 2 = Geräteblock
    const f2 = main.get(2);
    if (!f2 || f2[0].type !== 'bytes') return datapoints;

    const dev = parseProto(f2[0].value);
    // field 4 = Datenpunkte (repeated)
    const dps = dev.get(4) || [];

    for (const dp of dps) {
        if (dp.type !== 'bytes') continue;
        const dpFields = parseProto(dp.value);

        // field 1 = OBIS-ID (varint, 40-bit)
        const idField = dpFields.get(1);
        if (!idField) continue;
        const id = idField[0].raw; // BigInt

        // field 2 = Wert (varint, zigzag-signed) — optional (fehlt wenn 0)
        const valField = dpFields.get(2);
        const rawValue = valField ? valField[0].signed : 0n;

        datapoints.push({ id, rawValue });
    }
    return datapoints;
}

/**
 * Dekodiert eine KSEM Energiefluss (sumvalues) Protobuf-Nachricht.
 * Gibt ein Map<name, number> in Watt zurück.
 */
function decodeSumvaluesMessage(buf) {
    const result = {};
    const outer = parseProto(buf);

    const f1 = outer.get(1);
    if (!f1 || f1[0].type !== 'bytes') return result;

    const main = parseProto(f1[0].value);
    const f2 = main.get(2);
    if (!f2 || f2[0].type !== 'bytes') return result;

    const dev = parseProto(f2[0].value);

    // field 5 = Datenpunkte (repeated, name+value Struktur)
    const dps = dev.get(5) || [];

    for (const dp of dps) {
        if (dp.type !== 'bytes') continue;
        const dpFields = parseProto(dp.value);

        // field 1 = name (string)
        const nameField = dpFields.get(1);
        if (!nameField || nameField[0].type !== 'string') continue;
        const name = nameField[0].value;

        // field 2 = value container (bytes mit field 1 = varint)
        const valContainer = dpFields.get(2);
        if (!valContainer) {
            result[name] = null;
            continue;
        }
        const vc = valContainer[0];

        if (vc.type === 'string' && vc.value === '') {
            result[name] = null;
            continue;
        }

        if (vc.type === 'bytes') {
            const sub = parseProto(vc.value);
            const sv = sub.get(1);
            if (sv) {
                // int64 signed, Einheit mW → W
                const raw = BigInt.asIntN(64, sv[0].raw);
                result[name] = Number(raw) / 1000.0;
            } else {
                result[name] = null;
            }
        } else if (vc.type === 'varint') {
            result[name] = Number(vc.signed) / 1000.0;
        } else {
            result[name] = null;
        }
    }

    // field 4 = Config/Init Datenpunkte (Feldnamen als sub-messages)
    const dps4 = dev.get(4) || [];
    for (const dp of dps4) {
        if (dp.type !== 'bytes') continue;
        const dpFields = parseProto(dp.value);

        const nameField = dpFields.get(1);
        if (!nameField || nameField[0].type !== 'bytes') continue;
        const nameSub = parseProto(nameField[0].value);
        const nf = nameSub.get(1);
        if (!nf || nf[0].type !== 'string') continue;
        const name = nf[0].value;

        const valContainer = dpFields.get(2);
        if (!valContainer) continue;
        const vc = valContainer[0];
        if (vc.type === 'bytes') {
            const sub = parseProto(vc.value);
            const sv = sub.get(1);
            if (sv) {
                const raw = BigInt.asIntN(64, sv[0].raw);
                if (!(name in result)) {
                    result[name] = Number(raw) / 1000.0;
                }
            }
        }
    }

    // SOC: raw-Wert ist direkt der Prozentwert x10 (z.B. 1000 = 100%).
    // Wir haben oben /1000 angewendet -> ergibt direkt den Prozentwert (1000/1000=1... nein).
    // Tatsächlich: wenn SOC=100%, dann kommt raw=100 an (ganzzahlig).
    // Nach /1000 = 0.1. Wir muessen also *1000 = 100.
    if ('systemStateOfCharge' in result && result.systemStateOfCharge !== null) {
        result.systemStateOfCharge = Math.round(result.systemStateOfCharge * 1000);
    }

    return result;
}

module.exports = { parseProto, decodeSmartMeterMessage, decodeSumvaluesMessage };
