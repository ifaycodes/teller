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

const RELEASE_PROBE_LUA = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
`;

async function getState(channel, provider) {
    const raw = await redis.get(redisKey(channel, provider));
    return raw ? JSON.parse(raw) : { ...DEFAULT_STATE };
}

async function setState(channel, provider, data) {
    await redis.set(redisKey(channel, provider), JSON.stringify(data));
}

async function claimProbe(channel, provider) {
    const token = crypto.randomUUID();
    const acquired = await redis.set(probeKey(channel, provider), token , {NX: true, PX: config.circuitBreaker.probeLockMs});
    return acquired === 'OK' ? token : null;
}

async function releaseProbe(channel, provider, token) {
    if (!token) return;
    try {
        await redis.eval(RELEASE_PROBE_LUA, {
            keys: [probeKey(channel, provider)],
            arguments: [token]
        });
    } catch (err) {
        console.error(`[Circuit] Error releasing probe for ${channel}:${provider}`, err.message);
    }
}

export async function isOpen(channel, provider) {
    const data = await getState(channel, provider);
    const cooldownSeconds = config.circuitBreaker.cooldownSeconds;

    if (data.state === 'closed') return { open: false, probeToken: null };

    if (data.state === 'open') {
        const elapsed = (Date.now() - new Date(data.opened_at).getTime()) / 1000;
        if (elapsed < cooldownSeconds) {
            return { open: true, probeToken: null };       // to let the probe through
        }

        await setState(channel, provider, {...data, state: 'half-open'});
    }

    if (data.state === 'half-open') {
        const probeToken = await claimProbe(channel, provider);
        if (probeToken) {
            console.log(`[Circuit] ${channel}:${provider} -> half-open (probe acquired)`);
            return { open: false, probeToken };   // probe acquired, let it through
        }
        return { open: true, probeToken: null };    // probe acquired by another process, so subsequent attempts should be blocked until the probe is completed
    }
    return { open: false, probeToken: null };
}

export async function recordSuccess(channel, provider, probeToken = null) {
    const data = await getState(channel, provider);
    await releaseProbe(channel, provider, probeToken);

    if (data.state === 'half-open' || data.failure_count > 0) {
        await setState(channel, provider, { ...DEFAULT_STATE });
        console.log(`[Circuit] ${channel}:${provider} -> closed (probe succeeded)`);
    }
}

export async function recordFailure(channel, provider, probeToken = null) {
    const data = await getState(channel, provider);
    await releaseProbe(channel, provider, probeToken);
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