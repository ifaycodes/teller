import { createClient } from "redis";
import { config } from "../config/index.js";

let redis;
export async function startCircuit() {
    redis = new createClient({url: config.redisUrl})
    redis.on('error', (err) => console.error(`[Circuit] Redis Client Error`, err));
    await redis.connect();
    console.log(`[Circuit] Connected to Redis Client Successfully..`);
}

function redisKey(channel, provider) {
    return `circuit:${channel}:${provider}`;
}

function probeKey(channel, provider) {
    return `circuit:${channel}:${provider}:probe`;
}

const DEFAULT_STATE = {
    state: "closed",
    failure_count: 0,
    last_failure_at: null,
    opened_at: null
}

async function getState(channel, provider) {
    const raw = await redis.get(redisKey(channel, provider));
    return raw ? JSON.parse(raw) : { ...DEFAULT_STATE };
}

async function setState(channel, provider, data) {
    await redis.set(redisKey(channel, provider), JSON.stringify(data));
}

async function claimProbe(channel, provider) {
    const acquired = await redis.set(probeKey(channel, provider), '1', {NX: true, PX: config.circuitBreaker.probeLockMs});
    return acquired === 'OK';
}

export async function isOpen(channel, provider) {
    const data = await getState(channel, provider);
    const cooldownSeconds = config.circuitBreaker.cooldownSeconds;

    if (data.state === 'closed') return false;

    if (data.state === 'open') {
        const elapsed = (Date.now() - new Date(data.opened_at).getTime()) / 1000;
        if (elapsed >= cooldownSeconds) {
            await setState(channel, provider, {...data, state: 'half-open'});
            console.log(`[Circuit] ${channel}:${provider} -> half-open (probe allowed)`);
            return false;       // to let the probe through
        }
        console.log(`[Circuit] Channel - ${channel}:${provider} is open, pick another one`);
        return true;        // time has not elapsed, so it is still open
    }

    if (data.state === 'half-open') {
        const probeAcquired = await claimProbe(channel, provider);
        if (probeAcquired) {
            console.log(`[Circuit] ${channel}:${provider} -> half-open (probe acquired)`);
            return false;   // probe acquired, let it through
        }
        return true;    // probe acquired by another process, so subsequent attempts should be blocked until the probe is completed
    }
    return false;
}

export async function recordSuccess(channel, provider) {
    const data = await getState(channel, provider);
    await redis.del(probeKey(channel, provider)); // release probe lock if any
    if (data.state === 'half-open') {
        await setState(channel, provider, { ...DEFAULT_STATE });
        console.log(`[Circuit] ${channel}:${provider} -> closed (probe succeeded)`);
    }
}

export async function recordFailure(channel, provider) {
    const data = await getState(channel, provider);
    await redis.del(probeKey(channel, provider)); // release probe lock if any
    const failureThreshold = config.circuitBreaker.failureThreshold;

    if (data.state === 'half-open') {
        await setState(channel, provider, {
            ...data, state: 'open', opened_at: new Date().toISOString(),
        });
        console.log(`[Circuit] ${channel}:${provider} -> open (probe failed)`);
        return;
    }

    const failure_count = (data.failure_count || 0) + 1;
    const last_failure_at = new Date().toISOString();

    if (failure_count >= failureThreshold) {
        await setState(channel, provider, {
            state: 'open',
            failure_count,
            last_failure_at,
            opened_at: new Date().toISOString(),
        });
        console.log(`[Circuit] ${channel}:${provider} -> open (after ${failure_count} failures)`);
    } else {
        await setState(channel, provider, { ...data, failure_count, last_failure_at });
        console.log(`[Circuit] ${channel}:${provider} failure count: ${failure_count}`);
    }
}

export async function areAllProvidersOpen(channel, providers) {
    for (const provider of providers) {
        const open = await isOpen(channel, provider);
        if (!open) return false;
    }
    return true;
}