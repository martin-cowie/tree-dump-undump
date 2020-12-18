// Common.ts
import * as diffusion from "diffusion";
import {existsSync, mkdirSync} from 'fs';

/* A map of type number, to string */
const typeNumberToStrings =
    Object.fromEntries(Object.entries(diffusion.topics.TopicType).map( pair => {
        const [key, value] = pair;
        return [value.id, key];
    }));

/** Map a type number, into a type string */
export function mapType(topicType: diffusion.TopicType): string {
    return typeNumberToStrings[topicType.id];
}

export function pause(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export function buildOptions(url: URL): diffusion.SessionOptions {
    let result: diffusion.SessionOptions = {};
    if (url.host) {
        result.host = url.host;
    }
    if (url.port) {
        result.port = Number.parseInt(url.port);
    }
    result.secure = (url.protocol === 'wss:');
    if (url.username) {
        result.principal = url.username;
    }
    if (url.password) {
        result.credentials = url.password;
    }

    return result;
}

export async function buildSession(url: URL): Promise<diffusion.Session> {
    const result =  await diffusion.connect(buildOptions(url));
    console.error(`connected to ${url} as ${result.sessionID}`);
    return result;
}

/**
 * @param path the path property of a URL
 */
export function pathToSelector(path: string) {
    return path === '/'
        ?'?.*//'
        :`?${path.slice(1)}//`;
}


export function ensureDir(dir:string) {
    if (existsSync(dir)) {
        return;
    }
    return mkdirSync(dir, { recursive: true });
}


export async function getStats(session: diffusion.Session): Promise<[number, any]> {

    let request = await session.fetchRequest()
        // .first(BATCH_SIZE)
        .withProperties();

    let topicCount = 0;
    let topicStats = {};
    do {
        const response = await request.fetch('?.*//');
        const topicResults = response.results();
        topicCount += topicResults.length;

        topicStats = topicResults.reduce( (acc: any, value) => {
            const topicType = mapType(value.type());
            if (!acc[topicType]) {
                acc[topicType] = 1;
            } else {
                acc[topicType] += 1;
            }

            return acc;
        }, topicStats)

        if (response.hasMore()) {
            const lastTopicResult = topicResults[topicResults.length -1];
            request = request.after(lastTopicResult.path());
            await pause(1000);
        } else {
            break;
        }

    } while(true);

    return [topicCount, topicStats];
}