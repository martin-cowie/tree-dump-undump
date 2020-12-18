#!/usr/bin/env ts-node
// Copy one server to another

import * as diffusion from "diffusion";
import ProgressBar from 'progress';
import {mapType, pause, buildSession, pathToSelector, getStats} from './Common';

//TODO: optionally exclude reference topics (with property _VIEW)
const BATCH_SIZE = 4096;
const args = process.argv.slice(2);

function buildPath(prefix: string, path: string): string {
    return prefix ? prefix + '/' + path : path;
}

async function copyTopics(fromSession: diffusion.Session, toSession: diffusion.Session, pathPrefix: string, estimatedTopicMax: number): Promise<void> {
    var bar = new ProgressBar('  downloading [:bar] :rate topics/s :percent ETA :etas', {
        complete: '=',
        incomplete: ' ',
        width: 40,
        total: estimatedTopicMax
      });

    const Q = Math.round(estimatedTopicMax/100);

    let request = await fromSession.fetchRequest()
        .first(Q)
        .withProperties()
        .withValues(diffusion.datatypes.json());

    const jsonTopicSpec = new diffusion.topics.TopicSpecification(
        diffusion.topics.TopicType.JSON
    );

    let topicCount = 0;

    do {
        const response = await request.fetch(pathToSelector('/')); //TODO: use path from URL
        const topicResults = response.results();

        for(const r of topicResults) {
            //TODO: scope exists to record the CBOR size of JSON topics, via r.value().$buffer.length

            //FIXME: only copes with JSON for now (where .get() is required)
            const value = r.value().get();

            const newPath = buildPath(pathPrefix, r.path() as string);
            try {
                await toSession.topicUpdate.set(newPath,
                    diffusion.datatypes.json(),
                    value, {
                        specification: jsonTopicSpec
                    }
                );
            } catch(err) {
                console.log(`Cannot set topic ${newPath}: `, err);
                throw err;
            }
            topicCount++;

            if (topicCount % Q === 0) {
                // console.error(`Copied ${topicCount}/${estimatedTopicMax.toLocaleString()} `);
                bar.tick(Q);
            }
            bar.interrupt(`Set ${newPath}`);
        }


        if (!response.hasMore()) {
            break;
        }

        if (response.hasMore()) {
            const lastTopicResult = topicResults[topicResults.length -1];
            request = request.after(lastTopicResult.path());
            await pause(1000);
        } else {
            break;
        }
    } while(true);
}

async function main(args: string[]) {
    if (args.length < 2) {
        console.error(`wrong # args, try from-url to-url`);
        process.exit(1);
    }
    const fromURL = new URL(args[0]);
    const toURL = new URL(args[1]);

    const fromSession = await buildSession(fromURL);
    const toSession = await buildSession(toURL);

    const [topicCount, topicStats] = await getStats(fromSession);
    console.log(`Counted ${topicCount.toLocaleString()} topics`, topicStats, ` at ${fromURL}`);

    // Start the great copy
    await copyTopics(fromSession, toSession, toURL.pathname.slice(1), topicCount);
}

main(args).then(() => {
    process.exit(0);
}).catch(err => {
    console.error(err);
    process.exit(1);
});