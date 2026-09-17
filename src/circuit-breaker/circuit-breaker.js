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
        console.log(`[Circuit] Channel is open, pick another one`);
        return true;        // time has not elapsed, so it is still open
    }

    if (data.state === 'half-open') {
        return false;
    }
    return false;
}

export async function recordSuccess(channel, provider) {
    const data = await getState(channel, provider);
    if (data.state === 'half-open') {
        await setState(channel, provider, { ...DEFAULT_STATE });
    }
}

export async function recordFailure(channel, provider) {
    const data = await getState(channel, provider);
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