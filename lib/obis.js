'use strict';

// OBIS-Code Mapping für KSEM Momentanwerte (D=04) und Energiezähler (D=08)
// ID-Format: 01-00-CC-DD-00-FF als 40-bit Integer
// C-Gruppe = Messgröße, D-Gruppe = Messart (04=momentan, 08=energie)

const OBIS_C_MAP = {
    0x01: { name: 'active_power_plus',       unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x02: { name: 'active_power_minus',      unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x03: { name: 'reactive_power_plus',     unit: 'var', factor: 0.1,   role: 'value.power' },
    0x04: { name: 'reactive_power_minus',    unit: 'var', factor: 0.1,   role: 'value.power' },
    0x09: { name: 'apparent_power_plus',     unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x0a: { name: 'apparent_power_minus',    unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x0d: { name: 'power_factor',            unit: '',    factor: 0.001, role: 'value' },
    0x0e: { name: 'frequency',               unit: 'Hz',  factor: 0.001, role: 'value.frequency' },
    // L1
    0x15: { name: 'active_power_plus_l1',    unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x16: { name: 'active_power_minus_l1',   unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x17: { name: 'reactive_power_plus_l1',  unit: 'var', factor: 0.1,   role: 'value.power' },
    0x18: { name: 'reactive_power_minus_l1', unit: 'var', factor: 0.1,   role: 'value.power' },
    0x1d: { name: 'apparent_power_plus_l1',  unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x1e: { name: 'apparent_power_minus_l1', unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x1f: { name: 'current_l1',              unit: 'A',   factor: 0.001, role: 'value.current' },
    0x20: { name: 'voltage_l1',              unit: 'V',   factor: 0.001, role: 'value.voltage' },
    0x21: { name: 'power_factor_l1',         unit: '',    factor: 0.001, role: 'value' },
    // L2
    0x29: { name: 'active_power_plus_l2',    unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x2a: { name: 'active_power_minus_l2',   unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x2b: { name: 'reactive_power_plus_l2',  unit: 'var', factor: 0.1,   role: 'value.power' },
    0x2c: { name: 'reactive_power_minus_l2', unit: 'var', factor: 0.1,   role: 'value.power' },
    0x31: { name: 'apparent_power_plus_l2',  unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x32: { name: 'apparent_power_minus_l2', unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x33: { name: 'current_l2',              unit: 'A',   factor: 0.001, role: 'value.current' },
    0x34: { name: 'voltage_l2',              unit: 'V',   factor: 0.001, role: 'value.voltage' },
    0x35: { name: 'power_factor_l2',         unit: '',    factor: 0.001, role: 'value' },
    // L3
    0x3d: { name: 'active_power_plus_l3',    unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x3e: { name: 'active_power_minus_l3',   unit: 'W',   factor: 0.1,   role: 'value.power' },
    0x3f: { name: 'reactive_power_plus_l3',  unit: 'var', factor: 0.1,   role: 'value.power' },
    0x40: { name: 'reactive_power_minus_l3', unit: 'var', factor: 0.1,   role: 'value.power' },
    0x45: { name: 'apparent_power_plus_l3',  unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x46: { name: 'apparent_power_minus_l3', unit: 'VA',  factor: 0.1,   role: 'value.power' },
    0x47: { name: 'current_l3',              unit: 'A',   factor: 0.001, role: 'value.current' },
    0x48: { name: 'voltage_l3',              unit: 'V',   factor: 0.001, role: 'value.voltage' },
    0x49: { name: 'power_factor_l3',         unit: '',    factor: 0.001, role: 'value' },
};

// Energie-Suffix für D=08 Felder
const ENERGY_SUFFIX = '_kwh';
const ENERGY_UNIT   = 'kWh';
const ENERGY_SCALE  = 0.1 / 1000; // raw * 0.1 Wh → kWh

/**
 * Dekodiert einen OBIS-ID-Wert (40-bit BigInt oder Number) zu einem Datenpunkt-Descriptor.
 * @param {BigInt|number} id
 * @param {BigInt|number} rawValue  zigzag-decoded signed integer
 * @returns {{ stateId: string, value: number, unit: string, role: string } | null}
 */
function decodeObisDatapoint(id, rawValue) {
    // Sicher zu BigInt konvertieren
    const bigId = BigInt(id);
    const c = Number((bigId >> 24n) & 0xffn);
    const d = Number((bigId >> 16n) & 0xffn);

    const def = OBIS_C_MAP[c];
    if (!def) return null;

    if (d === 0x08) {
        // Energiezähler
        const value = Number(rawValue) * ENERGY_SCALE;
        return {
            stateId: def.name + ENERGY_SUFFIX,
            value:   Math.round(value * 1000) / 1000,
            unit:    ENERGY_UNIT,
            role:    'value.energy',
            write:   false,
        };
    } else if (d === 0x04) {
        // Momentanwert
        const value = Number(rawValue) * def.factor;
        return {
            stateId: def.name,
            value:   Math.round(value * 1000) / 1000,
            unit:    def.unit,
            role:    def.role,
            write:   false,
        };
    }
    return null;
}

module.exports = { decodeObisDatapoint, OBIS_C_MAP };
