'use strict';

const utils     = require('@iobroker/adapter-core');
const http      = require('http');
const WebSocket = require('ws');
const { decodeSmartMeterMessage, decodeSumvaluesMessage } = require('./lib/protobuf');
const { decodeObisDatapoint } = require('./lib/obis');

// -------------------------------------------------------------------
// Sumvalues State-Definitionen
// -------------------------------------------------------------------
const SUMVALUES_STATES = {
    gridPowerTotal:            { name: 'Netzleistung gesamt (+Bezug/-Einspeisung)', unit: 'W',  role: 'value.power',   type: 'number' },
    pvPowerTotal:              { name: 'PV-Leistung gesamt (DC)',                   unit: 'W',  role: 'value.power',   type: 'number' },
    pvPowerACSum:              { name: 'PV-Leistung gesamt (AC)',                   unit: 'W',  role: 'value.power',   type: 'number' },
    housePowerTotal:           { name: 'Hausverbrauch gesamt',                      unit: 'W',  role: 'value.power',   type: 'number' },
    homeConsumptionPV:         { name: 'Eigenverbrauch PV',                         unit: 'W',  role: 'value.power',   type: 'number' },
    homeConsumptionGrid:       { name: 'Hausverbrauch aus Netz',                    unit: 'W',  role: 'value.power',   type: 'number' },
    homeConsumptionBattery:    { name: 'Hausverbrauch aus Batterie',                unit: 'W',  role: 'value.power',   type: 'number' },
    batteryPowerTotal:         { name: 'Batterieleistung gesamt',                   unit: 'W',  role: 'value.power',   type: 'number' },
    batteryPowerACSum:         { name: 'Batterieleistung AC',                       unit: 'W',  role: 'value.power',   type: 'number' },
    inverterPowerTotal:        { name: 'Wechselrichterleistung gesamt',             unit: 'W',  role: 'value.power',   type: 'number' },
    wallboxPowerTotal:         { name: 'Wallbox-Leistung gesamt',                   unit: 'W',  role: 'value.power',   type: 'number' },
    wallboxConsumptionPV:      { name: 'Wallbox Eigenverbrauch PV',                 unit: 'W',  role: 'value.power',   type: 'number' },
    wallboxConsumptionGrid:    { name: 'Wallbox Verbrauch aus Netz',                unit: 'W',  role: 'value.power',   type: 'number' },
    wallboxConsumptionBattery: { name: 'Wallbox Verbrauch aus Batterie',            unit: 'W',  role: 'value.power',   type: 'number' },
    systemStateOfCharge:       { name: 'Batterie SOC',                              unit: '%',  role: 'value.battery', type: 'number' },
    inverterCurtailment:       { name: 'Wechselrichter Abregelung aktiv',           unit: '',   role: 'value',         type: 'number' },
    auxiliaryPowerTotal:       { name: 'Hilfsverbrauch gesamt',                     unit: 'W',  role: 'value.power',   type: 'number' },
    smartGridLimit:            { name: 'Smart Grid Limit',                          unit: 'W',  role: 'value.power',   type: 'number' },
    batteryChargeLimit:        { name: 'Batterie Ladelimit',                        unit: 'W',  role: 'value.power',   type: 'number' },
    batteryDischargeLimit:     { name: 'Batterie Entladelimit',                     unit: 'W',  role: 'value.power',   type: 'number' },
};

const WALLBOX_POLL_INTERVAL = 5000; // ms

// -------------------------------------------------------------------
class KostalKsem extends utils.Adapter {
    constructor(options = {}) {
        super({ ...options, name: 'kostal-ksem' });

        this._token         = null;
        this._tokenExpiry   = 0;
        this._wsSmartMeter  = null;
        this._wsSumvalues   = null;
        this._reconnTimer   = null;
        this._wallboxTimer  = null;
        this._statesCreated = false;
        this._wallboxIds    = new Set();

        this.on('ready',  this._onReady.bind(this));
        this.on('unload', this._onUnload.bind(this));
    }

    // ----------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------
    async _onReady() {
        this.log.info('KOSTAL KSEM Adapter gestartet');
        this.setState('info.connection', false, true);
        await this._connect();
    }

    async _onUnload(callback) {
        this.log.info('Adapter wird gestoppt');
        this._clearReconnTimer();
        this._clearWallboxTimer();
        this._closeWs(this._wsSmartMeter);
        this._closeWs(this._wsSumvalues);
        this.setState('info.connection', false, true);
        callback();
    }

    // ----------------------------------------------------------------
    // Verbindungsaufbau
    // ----------------------------------------------------------------
    async _connect() {
        try {
            await this._login();
            await this._ensureStates();
            this._openSmartMeterWs();
            this._openSumvaluesWs();
            this._startWallboxPolling();
        } catch (err) {
            this.log.error(`Verbindungsfehler: ${err.message}`);
            this._scheduleReconnect();
        }
    }

    _scheduleReconnect(delay = 30000) {
        this._clearReconnTimer();
        this._clearWallboxTimer();
        this.setState('info.connection', false, true);
        this.log.info(`Neuverbindung in ${delay / 1000}s ...`);
        this._reconnTimer = setTimeout(() => this._connect(), delay);
    }

    _clearReconnTimer() {
        if (this._reconnTimer) { clearTimeout(this._reconnTimer); this._reconnTimer = null; }
    }

    // ----------------------------------------------------------------
    // Login / Token
    // ----------------------------------------------------------------
    async _login() {
        const { host, password } = this.config;
        const pwEncoded = encodeURIComponent(password);
        const body = `grant_type=password&client_id=emos&client_secret=56951025&username=admin&password=${pwEncoded}`;

        this.log.debug(`Login bei http://${host}/api/web-login/token`);
        const data = await this._httpPost(`http://${host}/api/web-login/token`, body);
        const json = JSON.parse(data);

        if (!json.access_token) throw new Error(`Login fehlgeschlagen: ${JSON.stringify(json)}`);

        this._token       = json.access_token;
        this._tokenExpiry = Date.now() + (json.expires_in - 3600) * 1000;
        this.log.info(`Login erfolgreich, Token gültig für ${Math.round(json.expires_in / 3600)}h`);

        await this._fetchDeviceInfo();
    }

    async _ensureToken() {
        if (Date.now() > this._tokenExpiry) {
            this.log.info('Token abgelaufen, erneuere ...');
            await this._login();
        }
    }

    // ----------------------------------------------------------------
    // Geräteinfo
    // ----------------------------------------------------------------
    async _fetchDeviceInfo() {
        const { host } = this.config;
        try {
            const data = await this._httpGet(`http://${host}/api/device-settings`, this._token);
            const info = JSON.parse(data);
            await this.setStateAsync('info.serial',   { val: info.Serial,          ack: true });
            await this.setStateAsync('info.firmware', { val: info.FirmwareVersion,  ack: true });
            await this.setStateAsync('info.hostname', { val: info.hostname,         ack: true });
            await this.setStateAsync('info.mac',      { val: info.Mac,              ack: true });
            this.log.info(`Gerät: ${info.ProductName} SN=${info.Serial} FW=${info.FirmwareVersion}`);
        } catch (err) {
            this.log.warn(`Geräteinfo konnte nicht geladen werden: ${err.message}`);
        }
    }

    // ----------------------------------------------------------------
    // States anlegen
    // ----------------------------------------------------------------
    async _ensureStates() {
        if (this._statesCreated) return;
        this._statesCreated = true;

        for (const [id, name] of [
            ['info',       'Geräteinfo'],
            ['smartmeter', 'KSEM Messwerte'],
            ['energyflow', 'Energiefluss'],
            ['wallbox',    'Wallbox'],
        ]) {
            await this.setObjectNotExistsAsync(id, {
                type: 'channel', common: { name }, native: {},
            });
        }

        await this._createState('info.serial',    'Seriennummer',     'string', 'text');
        await this._createState('info.firmware',  'Firmware-Version', 'string', 'text');
        await this._createState('info.hostname',  'Hostname',         'string', 'text');
        await this._createState('info.mac',       'MAC-Adresse',      'string', 'text');

        for (const [key, def] of Object.entries(SUMVALUES_STATES)) {
            await this._createState(`energyflow.${key}`, def.name, def.type, def.role, def.unit);
        }
    }

    async _createState(id, name, type, role, unit = '') {
        await this.setObjectNotExistsAsync(id, {
            type: 'state',
            common: { name, type, role, unit, read: true, write: false },
            native: {},
        });
    }

    async _ensureSmartMeterState(stateId, unit, role) {
        await this.setObjectNotExistsAsync(`smartmeter.${stateId}`, {
            type: 'state',
            common: { name: stateId, type: 'number', role, unit, read: true, write: false },
            native: {},
        });
    }

    async _ensureWallboxStates(uuid) {
        if (this._wallboxIds.has(uuid)) return;
        this._wallboxIds.add(uuid);

        const base = `wallbox.${uuid}`;
        await this.setObjectNotExistsAsync(base, {
            type: 'channel', common: { name: `Wallbox ${uuid}` }, native: {},
        });

        for (const [id, name, type, role, unit] of [
            [`${base}.connected`,   'Fahrzeug verbunden',       'boolean', 'indicator',     ''],
            [`${base}.charging`,    'Lädt gerade',              'boolean', 'indicator',     ''],
            [`${base}.min_current`, 'Minimalstrom',             'number',  'value.current', 'A'],
            [`${base}.max_current`, 'Maximalstrom',             'number',  'value.current', 'A'],
            [`${base}.phases_l1`,   'Phase L1 aktiv',           'boolean', 'indicator',     ''],
            [`${base}.phases_l2`,   'Phase L2 aktiv',           'boolean', 'indicator',     ''],
            [`${base}.phases_l3`,   'Phase L3 aktiv',           'boolean', 'indicator',     ''],
            [`${base}.phase_usage`, 'Phasennutzung (1 oder 3)', 'number',  'value',         ''],
        ]) {
            await this._createState(id, name, type, role, unit);
        }
    }

    // ----------------------------------------------------------------
    // WebSocket: Smart Meter
    // ----------------------------------------------------------------
    _openSmartMeterWs() {
        const url = `ws://${this.config.host}/api/data-transfer/ws/protobuf/gdr/local/values/smart-meter`;
        this._wsSmartMeter = this._openWs(url, 'smart-meter', async (buf) => {
            try {
                const dps = decodeSmartMeterMessage(buf);
                for (const { id, rawValue } of dps) {
                    const dp = decodeObisDatapoint(id, rawValue);
                    if (!dp) continue;
                    await this._ensureSmartMeterState(dp.stateId, dp.unit, dp.role);
                    this.setState(`smartmeter.${dp.stateId}`, { val: dp.value, ack: true });
                }
            } catch (err) {
                this.log.warn(`Smart-Meter Dekodierungsfehler: ${err.message}`);
            }
        });
    }

    // ----------------------------------------------------------------
    // WebSocket: Energiefluss (sumvalues)
    // ----------------------------------------------------------------
    _openSumvaluesWs() {
        const url = `ws://${this.config.host}/api/data-transfer/ws/protobuf/gdr/local/values/kostal-energyflow/sumvalues`;
        this._wsSumvalues = this._openWs(url, 'sumvalues', async (buf) => {
            try {
                const values = decodeSumvaluesMessage(buf);
                for (const [key, val] of Object.entries(values)) {
                    if (val === null || !(key in SUMVALUES_STATES)) continue;
                    this.setState(`energyflow.${key}`, { val: Math.round(val * 10) / 10, ack: true });
                }
            } catch (err) {
                this.log.warn(`Sumvalues Dekodierungsfehler: ${err.message}`);
            }
        });
    }

    // ----------------------------------------------------------------
    // Wallbox Polling
    // ----------------------------------------------------------------
    _startWallboxPolling() {
        this._clearWallboxTimer();
        this._pollWallbox();
        this._wallboxTimer = setInterval(() => this._pollWallbox(), WALLBOX_POLL_INTERVAL);
    }

    _clearWallboxTimer() {
        if (this._wallboxTimer) { clearInterval(this._wallboxTimer); this._wallboxTimer = null; }
    }

    async _pollWallbox() {
        const { host } = this.config;
        try {
            await this._ensureToken();

            const [phaseData, evData] = await Promise.all([
                this._httpGet(`http://${host}/api/e-mobility/config/phaseswitching`, this._token),
                this._httpGet(`http://${host}/api/e-mobility/evparameterlist`, this._token),
            ]);

            const phaseJson = JSON.parse(phaseData);
            const evJson    = JSON.parse(evData);

            for (const [uuid, params] of Object.entries(evJson)) {
                await this._ensureWallboxStates(uuid);
                const base = `wallbox.${uuid}`;

                const connected = !!(params.phases_used?.total ||
                                     params.min_current > 0 ||
                                     params.max_current > 0);
                const charging  = !!(params.phases_used?.l1 ||
                                     params.phases_used?.l2 ||
                                     params.phases_used?.l3);

                this.setState(`${base}.connected`,   { val: connected,                  ack: true });
                this.setState(`${base}.charging`,    { val: charging,                   ack: true });
                this.setState(`${base}.min_current`, { val: params.min_current ?? 0,    ack: true });
                this.setState(`${base}.max_current`, { val: params.max_current ?? 0,    ack: true });
                this.setState(`${base}.phases_l1`,   { val: !!params.phases_used?.l1,   ack: true });
                this.setState(`${base}.phases_l2`,   { val: !!params.phases_used?.l2,   ack: true });
                this.setState(`${base}.phases_l3`,   { val: !!params.phases_used?.l3,   ack: true });
                this.setState(`${base}.phase_usage`, { val: phaseJson.phase_usage ?? 0, ack: true });
            }
        } catch (err) {
            this.log.warn(`Wallbox Polling Fehler: ${err.message}`);
        }
    }

    // ----------------------------------------------------------------
    // WebSocket Helper
    // ----------------------------------------------------------------
    _openWs(url, label, onMessage) {
        let ws;
        try {
            ws = new WebSocket(url);
        } catch (err) {
            this.log.error(`WebSocket ${label} konnte nicht geöffnet werden: ${err.message}`);
            return null;
        }

        ws.on('open', () => {
            this.log.debug(`${label} WebSocket verbunden, sende Token`);
            ws.send(`Bearer ${this._token}`);
            this.setState('info.connection', true, true);
        });

        ws.on('message', (data) => {
            if (Buffer.isBuffer(data)) onMessage(data);
        });

        ws.on('error', (err) => {
            this.log.warn(`${label} WebSocket Fehler: ${err.message}`);
        });

        ws.on('close', (code, reason) => {
            this.log.warn(`${label} WebSocket geschlossen (${code}): ${reason}`);
            this.setState('info.connection', false, true);
            this._scheduleReconnect();
        });

        return ws;
    }

    _closeWs(ws) {
        if (ws) { try { ws.terminate(); } catch {} }
    }

    // ----------------------------------------------------------------
    // HTTP Helpers
    // ----------------------------------------------------------------
    _httpPost(url, body) {
        return new Promise((resolve, reject) => {
            const { hostname, port, pathname } = new URL(url);
            const req = http.request({
                hostname, port: port || 80, path: pathname, method: 'POST',
                headers: {
                    'Content-Type':   'application/x-www-form-urlencoded',
                    'Content-Length': Buffer.byteLength(body),
                },
            }, (res) => {
                let data = '';
                res.on('data', c => data += c);
                res.on('end', () => resolve(data));
            });
            req.on('error', reject);
            req.write(body);
            req.end();
        });
    }

    _httpGet(url, token) {
        return new Promise((resolve, reject) => {
            const { hostname, port, pathname } = new URL(url);
            const req = http.request({
                hostname, port: port || 80, path: pathname, method: 'GET',
                headers: token ? { 'Authorization': `Bearer ${token}` } : {},
            }, (res) => {
                let data = '';
                res.on('data', c => data += c);
                res.on('end', () => resolve(data));
            });
            req.on('error', reject);
            req.end();
        });
    }
}

// ----------------------------------------------------------------
// Start
// ----------------------------------------------------------------
if (require.main !== module) {
    module.exports = (options) => new KostalKsem(options);
} else {
    new KostalKsem();
}
