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

// OBIS Gruppe → Konfigurationsschlüssel
function getSmGroup(stateId) {
    const isKwh = stateId.endsWith('_kwh');
    const isL1  = stateId.includes('_l1');
    const isL2  = stateId.includes('_l2');
    const isL3  = stateId.includes('_l3');
    if (isKwh) {
        if (isL1) return 'enableSmEnergyL1';
        if (isL2) return 'enableSmEnergyL2';
        if (isL3) return 'enableSmEnergyL3';
        return 'enableSmEnergyTotal';
    } else {
        if (isL1) return 'enableSmL1';
        if (isL2) return 'enableSmL2';
        if (isL3) return 'enableSmL3';
        return 'enableSmTotal';
    }
}

const VALID_CHARGEMODES = ['lock', 'grid', 'pv', 'hybrid'];

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
        this._stateTimer    = null;
        this._statesCreated = false;
        this._wallboxIds    = new Set();
        this._stateCache    = new Map();
        this._objectCache   = new Set();

        this.on('ready',        this._onReady.bind(this));
        this.on('unload',       this._onUnload.bind(this));
        this.on('stateChange',  this._onStateChange.bind(this));
    }

    _cfg(key, def) {
        const v = this.config[key];
        return (v === undefined || v === null) ? def : v;
    }

    _smThrottleMs() {
        return Math.max(1, this._cfg('updateInterval', 5)) * 1000;
    }

    async _onReady() {
        this.log.info('KOSTAL KSEM Adapter gestartet');
        this.setState('info.connection', false, true);
        this.subscribeStates('wallbox.*.chargemode');
        this.subscribeStates('wallbox.*.pause');
        this.subscribeStates('wallbox.*.phase_usage');
        await this._connect();
    }

    async _onUnload(callback) {
        this._clearReconnTimer();
        this._clearWallboxTimer();
        this._clearStateTimer();
        this._closeWs(this._wsSmartMeter);
        this._closeWs(this._wsSumvalues);
        this.setState('info.connection', false, true);
        callback();
    }

    // ----------------------------------------------------------------
    // State-Change Handler — schreibbare States
    // ----------------------------------------------------------------
    async _onStateChange(id, state) {
        if (!state || state.ack) return; // nur unacknowledged (von außen geschrieben)
        const parts = id.split('.');
        // id z.B. "kostal-ksem.0.wallbox.2a1baa2c-.../chargemode"
        const channel = parts[parts.length - 1];
        const uuid    = parts[parts.length - 2];

        try {
            await this._ensureToken();
            if (channel === 'chargemode') {
                await this._setChargemode(uuid, String(state.val).toLowerCase());
            } else if (channel === 'pause') {
                await this._setPause(uuid, Boolean(state.val));
            } else if (channel === 'phase_usage') {
                await this._setPhaseswitching(Number(state.val));
            }
        } catch (err) {
            this.log.error(`State-Change Fehler (${id}): ${err.message}`);
        }
    }

    // ----------------------------------------------------------------
    // Verbindungsaufbau
    // ----------------------------------------------------------------
    async _connect() {
        try {
            await this._login();
            await this._ensureStates();
            this._openSmartMeterWs();
            if (this._cfg('enableEnergyflow', true)) this._openSumvaluesWs();
            if (this._cfg('enableWallbox',    true)) {
                this._startWallboxPolling(); // startet auch _startStatePolling nach erstem Poll
            }
        } catch (err) {
            this.log.error(`Verbindungsfehler: ${err.message}`);
            this._scheduleReconnect();
        }
    }

    _scheduleReconnect(delay = 30000) {
        this._clearReconnTimer();
        this._clearWallboxTimer();
        this._clearStateTimer();
        this.setState('info.connection', false, true);
        this.log.info(`Neuverbindung in ${delay / 1000}s ...`);
        this._reconnTimer = setTimeout(() => this._connect(), delay);
    }

    _clearReconnTimer() {
        if (this._reconnTimer) { clearTimeout(this._reconnTimer); this._reconnTimer = null; }
    }

    // ----------------------------------------------------------------
    // Gecachtes setState — schreibt nur wenn Wert sich geändert hat
    // und das Throttle-Intervall abgelaufen ist
    // ----------------------------------------------------------------
    _setStateCached(id, val, throttleMs = 0) {
        const now = Date.now();
        const cached = this._stateCache.get(id);
        if (cached !== undefined && cached.val === val) return; // kein Wertewechsel
        if (throttleMs > 0 && cached !== undefined && (now - cached.ts) < throttleMs) return; // zu früh
        this._stateCache.set(id, { val, ts: now });
        this.setState(id, { val, ack: true });
    }

    async _ensureObject(id, obj) {
        if (this._objectCache.has(id)) return;
        this._objectCache.add(id);
        await this.setObjectNotExistsAsync(id, obj);
    }

    async _ensureChannel(id, name) {
        await this._ensureObject(id, { type: 'channel', common: { name }, native: {} });
    }

    async _ensureState(id, name, type, role, unit = '', write = false) {
        await this._ensureObject(id, {
            type: 'state',
            common: { name, type, role, unit, read: true, write },
            native: {},
        });
    }

    // ----------------------------------------------------------------
    // Login / Token
    // ----------------------------------------------------------------
    async _login() {
        const { host, password } = this.config;
        const body = `grant_type=password&client_id=emos&client_secret=56951025&username=admin&password=${encodeURIComponent(password)}`;
        const data = await this._httpPost(`http://${host}/api/web-login/token`, body);
        const json = JSON.parse(data);
        if (!json.access_token) throw new Error(`Login fehlgeschlagen: ${JSON.stringify(json)}`);

        this._token = json.access_token;

        // KSEM liefert expires_in manchmal in ms (>100000), manchmal in Sekunden
        const expiresIn = json.expires_in || 3600;
        const expiresInSec = expiresIn > 100000 ? Math.round(expiresIn / 1000) : expiresIn;
        this._tokenExpiry = Date.now() + (expiresInSec - 60) * 1000;

        this.log.info(`Login OK, Token gültig für ${Math.round(expiresInSec / 60)}min`);
        await this._fetchDeviceInfo();
    }

    async _ensureToken() {
        if (Date.now() > this._tokenExpiry) {
            this.log.info('Token abgelaufen, erneuere ...');
            await this._login();
        }
    }

    async _fetchDeviceInfo() {
        try {
            const data = await this._httpGet(`http://${this.config.host}/api/device-settings`, this._token);
            const info = JSON.parse(data);
            this._setStateCached('info.serial',   info.Serial);
            this._setStateCached('info.firmware', info.FirmwareVersion);
            this._setStateCached('info.hostname', info.hostname);
            this._setStateCached('info.mac',      info.Mac);
            this.log.info(`Gerät: ${info.ProductName} SN=${info.Serial} FW=${info.FirmwareVersion}`);
        } catch (err) {
            this.log.warn(`Geräteinfo Fehler: ${err.message}`);
        }
    }

    // ----------------------------------------------------------------
    // States einmalig anlegen
    // ----------------------------------------------------------------
    async _ensureStates() {
        if (this._statesCreated) return;
        this._statesCreated = true;

        const channels = [['info', 'Geräteinfo'], ['smartmeter', 'KSEM Messwerte']];
        if (this._cfg('enableEnergyflow', true)) channels.push(['energyflow', 'Energiefluss']);
        if (this._cfg('enableWallbox',    true)) channels.push(['wallbox',    'Wallbox']);
        for (const [id, name] of channels) await this._ensureChannel(id, name);

        await this._ensureState('info.serial',   'Seriennummer',     'string', 'text');
        await this._ensureState('info.firmware', 'Firmware-Version', 'string', 'text');
        await this._ensureState('info.hostname', 'Hostname',         'string', 'text');
        await this._ensureState('info.mac',      'MAC-Adresse',      'string', 'text');

        if (this._cfg('enableEnergyflow', true)) {
            for (const [key, def] of Object.entries(SUMVALUES_STATES)) {
                await this._ensureState(`energyflow.${key}`, def.name, def.type, def.role, def.unit);
            }
        }
    }

    async _ensureSmartMeterState(stateId, unit, role) {
        await this._ensureState(`smartmeter.${stateId}`, stateId, 'number', role, unit);
    }

    async _ensureWallboxStates(uuid) {
        if (this._wallboxIds.has(uuid)) return;
        this._wallboxIds.add(uuid);
        const base = `wallbox.${uuid}`;
        await this._ensureChannel(base, `Wallbox ${uuid}`);

        // Lesbare States
        for (const [id, name, type, role, unit] of [
            [`${base}.connected`,       'Fahrzeug verbunden',         'boolean', 'indicator',     ''],
            [`${base}.charging`,        'Lädt gerade',                'boolean', 'indicator',     ''],
            [`${base}.min_current`,     'Minimalstrom',               'number',  'value.current', 'A'],
            [`${base}.max_current`,     'Maximalstrom',               'number',  'value.current', 'A'],
            [`${base}.phases_l1`,       'Phase L1 aktiv',             'boolean', 'indicator',     ''],
            [`${base}.phases_l2`,       'Phase L2 aktiv',             'boolean', 'indicator',     ''],
            [`${base}.phases_l3`,       'Phase L3 aktiv',             'boolean', 'indicator',     ''],
            [`${base}.charging_power_w`,'Ladeleistung',               'number',  'value.power',   'W'],
            [`${base}.available_power_w`,'Verfügbare Leistung',       'number',  'value.power',   'W'],
        ]) {
            await this._ensureState(id, name, type, role, unit, false);
        }

        // Schreibbare States
        await this._ensureState(`${base}.chargemode`,  'Lademodus (pv/hybrid/grid/lock)', 'string',  'text',      '', true);
        await this._ensureState(`${base}.pause`,       'Laden pausieren',                 'boolean', 'switch',    '', true);
        await this._ensureState(`${base}.phase_usage`, 'Phasen (0=3-phasig, 1=1-phasig)', 'number',  'value',     '', true);
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
                    const group = getSmGroup(dp.stateId);
                    if (!this._cfg(group, true)) continue;
                    await this._ensureSmartMeterState(dp.stateId, dp.unit, dp.role);
                    this._setStateCached(`smartmeter.${dp.stateId}`, dp.value, this._smThrottleMs());
                }
            } catch (err) {
                this.log.warn(`Smart-Meter Fehler: ${err.message}`);
            }
        });
    }

    // ----------------------------------------------------------------
    // WebSocket: Energiefluss
    // ----------------------------------------------------------------
    _openSumvaluesWs() {
        const url = `ws://${this.config.host}/api/data-transfer/ws/protobuf/gdr/local/values/kostal-energyflow/sumvalues`;
        this._wsSumvalues = this._openWs(url, 'sumvalues', async (buf) => {
            try {
                const values = decodeSumvaluesMessage(buf);
                for (const [key, val] of Object.entries(values)) {
                    if (val === null || !(key in SUMVALUES_STATES)) continue;
                    this._setStateCached(`energyflow.${key}`, Math.round(val * 10) / 10, this._smThrottleMs());
                }
            } catch (err) {
                this.log.warn(`Sumvalues Fehler: ${err.message}`);
            }
        });
    }

    // ----------------------------------------------------------------
    // Wallbox Polling (evparameterlist + phaseswitching)
    // ----------------------------------------------------------------
    _startWallboxPolling() {
        this._clearWallboxTimer();
        // Erst pollWallbox (legt States an), dann pollState starten
        this._pollWallbox().then(() => this._startStatePolling());
        const interval = Math.max(2, this._cfg('wallboxPollInterval', 5)) * 1000;
        this._wallboxTimer = setInterval(() => this._pollWallbox(), interval);
    }

    _clearWallboxTimer() {
        if (this._wallboxTimer) { clearInterval(this._wallboxTimer); this._wallboxTimer = null; }
    }

    async _pollWallbox() {
        try {
            await this._ensureToken();
            const [phaseData, evData] = await Promise.all([
                this._httpGet(`http://${this.config.host}/api/e-mobility/config/phaseswitching`, this._token),
                this._httpGet(`http://${this.config.host}/api/e-mobility/evparameterlist`, this._token),
            ]);
            const phaseJson = JSON.parse(phaseData);
            const evJson    = JSON.parse(evData);

            for (const [uuid, params] of Object.entries(evJson)) {
                await this._ensureWallboxStates(uuid);
                const base      = `wallbox.${uuid}`;
                const connected = !!(params.phases_used?.total || params.min_current > 0 || params.max_current > 0);
                const charging  = !!(params.phases_used?.l1 || params.phases_used?.l2 || params.phases_used?.l3);

                this._setStateCached(`${base}.connected`,   connected);
                this._setStateCached(`${base}.charging`,    charging);
                this._setStateCached(`${base}.min_current`, params.min_current ?? 0);
                this._setStateCached(`${base}.max_current`, params.max_current ?? 0);
                this._setStateCached(`${base}.phases_l1`,   !!params.phases_used?.l1);
                this._setStateCached(`${base}.phases_l2`,   !!params.phases_used?.l2);
                this._setStateCached(`${base}.phases_l3`,   !!params.phases_used?.l3);
                this._setStateCached(`${base}.phase_usage`, phaseJson.phase_usage ?? 0);
            }
        } catch (err) {
            this.log.warn(`Wallbox Polling Fehler: ${err.message}`);
        }
    }

    // ----------------------------------------------------------------
    // State Polling (e-mobility/state → Ladeleistung + verfügbare Leistung)
    // ----------------------------------------------------------------
    _startStatePolling() {
        this._clearStateTimer();
        this._pollState();
        this._stateTimer = setInterval(() => this._pollState(), 30000); // alle 30s
    }

    _clearStateTimer() {
        if (this._stateTimer) { clearInterval(this._stateTimer); this._stateTimer = null; }
    }

    async _pollState() {
        try {
            await this._ensureToken();
            const data = await this._httpGet(`http://${this.config.host}/api/e-mobility/state`, this._token);
            const d    = JSON.parse(data);
            if (!d || typeof d !== 'object') return;

            const ev      = d.EvChargingPower   || {};
            const curtail = d.CurtailmentSetpoint || {};

            // mW → W
            const watt  = Math.round((ev.total || 0) / 1000);
            // mA × 230V → W
            const avail = Math.round(((curtail.l1 || 0) + (curtail.l2 || 0) + (curtail.l3 || 0)) / 1000 * 230);

            // Für alle bekannten Wallbox-UUIDs setzen
            for (const uuid of this._wallboxIds) {
                this._setStateCached(`wallbox.${uuid}.charging_power_w`,  watt);
                this._setStateCached(`wallbox.${uuid}.available_power_w`, avail);
            }
        } catch (err) {
            this.log.warn(`State Polling Fehler: ${err.message}`);
        }
    }

    // ----------------------------------------------------------------
    // Wallbox Steuerung
    // ----------------------------------------------------------------
    async _setChargemode(uuid, mode) {
        if (!VALID_CHARGEMODES.includes(mode)) {
            this.log.warn(`Ungültiger Lademodus: ${mode}`);
            return;
        }
        const body = JSON.stringify({
            mode,
            minpvpowerquota:      100,
            mincharginpowerquota: 0,
            controlledby:         0,
        });
        const code = await this._httpPut(
            `http://${this.config.host}/api/e-mobility/config/chargemode`,
            body
        );
        if (code === 204) {
            this.log.info(`Lademodus gesetzt: ${mode}`);
            this._setStateCached(`wallbox.${uuid}.chargemode`, mode);
        } else {
            this.log.warn(`Chargemode PUT fehlgeschlagen: HTTP ${code}`);
        }
    }

    async _setPause(uuid, pause) {
        const body = JSON.stringify({ pause });
        const code = await this._httpPut(
            `http://${this.config.host}/api/e-mobility/evse/${uuid}/setcharging`,
            body
        );
        if (code === 204) {
            this.log.info(`Laden ${pause ? 'pausiert' : 'gestartet'}`);
            this._setStateCached(`wallbox.${uuid}.pause`, pause);
        } else {
            this.log.warn(`Pause PUT fehlgeschlagen: HTTP ${code}`);
        }
    }

    async _setPhaseswitching(phase) {
        const body = JSON.stringify({ phase_usage: phase });
        const code = await this._httpPut(
            `http://${this.config.host}/api/e-mobility/config/phaseswitching`,
            body
        );
        if (code === 204) {
            this.log.info(`Phasen gesetzt: ${phase === 0 ? '3-phasig' : '1-phasig'}`);
        } else {
            this.log.warn(`Phaseswitching PUT fehlgeschlagen: HTTP ${code}`);
        }
    }

    // ----------------------------------------------------------------
    // WebSocket Helper
    // ----------------------------------------------------------------
    _openWs(url, label, onMessage) {
        let ws;
        try { ws = new WebSocket(url); } catch (err) {
            this.log.error(`WS ${label} Fehler: ${err.message}`); return null;
        }
        ws.on('open',    ()    => { this.log.debug(`${label} verbunden`); ws.send(`Bearer ${this._token}`); this.setState('info.connection', true, true); });
        ws.on('message', (data) => { if (Buffer.isBuffer(data)) onMessage(data); });
        ws.on('error',   (err)  => { this.log.warn(`${label} WS Fehler: ${err.message}`); });
        ws.on('close',   (code) => { this.log.warn(`${label} WS geschlossen (${code})`); this.setState('info.connection', false, true); this._scheduleReconnect(); });
        return ws;
    }

    _closeWs(ws) { if (ws) { try { ws.terminate(); } catch {} } }

    // ----------------------------------------------------------------
    // HTTP Helpers
    // ----------------------------------------------------------------
    _httpPost(url, body) {
        return new Promise((resolve, reject) => {
            const { hostname, port, pathname } = new URL(url);
            const req = http.request({
                hostname, port: port || 80, path: pathname, method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
            }, (res) => { let d = ''; res.on('data', c => d += c); res.on('end', () => resolve(d)); });
            req.on('error', reject); req.write(body); req.end();
        });
    }

    _httpGet(url, token) {
        return new Promise((resolve, reject) => {
            const { hostname, port, pathname } = new URL(url);
            const req = http.request({
                hostname, port: port || 80, path: pathname, method: 'GET',
                headers: token ? { 'Authorization': `Bearer ${token}` } : {},
            }, (res) => { let d = ''; res.on('data', c => d += c); res.on('end', () => resolve(d)); });
            req.on('error', reject); req.end();
        });
    }

    _httpPut(url, body) {
        return new Promise((resolve, reject) => {
            const { hostname, port, pathname } = new URL(url);
            const req = http.request({
                hostname, port: port || 80, path: pathname, method: 'PUT',
                headers: {
                    'Authorization':  `Bearer ${this._token}`,
                    'Content-Type':   'application/json',
                    'Content-Length': Buffer.byteLength(body),
                },
            }, (res) => {
                res.resume(); // Body verwerfen
                res.on('end', () => resolve(res.statusCode));
            });
            req.on('error', reject); req.write(body); req.end();
        });
    }
}

if (require.main !== module) {
    module.exports = (options) => new KostalKsem(options);
} else {
    new KostalKsem();
}
