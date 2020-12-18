#!/usr/bin/env ts-node
import * as diffusion from "diffusion";
import {pathToSelector, pause, buildSession} from './Common';

// List the topics on a remote server

const BATCH_SIZE = 4096;
const args = process.argv.slice(2);

function buildPath(prefix: string, path: string): string {
    return prefix ? prefix + '/' + path : path;
}

async function listTopics(session: diffusion.Session, path: string): Promise<void> {

    let topicCount = 0;
    let request = session.fetchRequest()
        .first(BATCH_SIZE)
        .withProperties()

    const jsonTopicSpec = new diffusion.topics.TopicSpecification(
        diffusion.topics.TopicType.JSON
    );

    const selector = pathToSelector(path);

    do {
        const response = await request.fetch(selector);
        const topicResults = response.results();

        for(const r of topicResults) {
            console.log(r.path(), r.specification().properties);
        }
        topicCount += topicResults.length;

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

    console.log(`Total of ${topicCount} topics`);
}

async function main(args: string[]) {
    if (args.length < 1) {
        console.error(`wrong # args, try url`);
        process.exit(1);
    }
    const url = new URL(args[0]);
    const session = await buildSession(url);

    await listTopics(session, url.pathname);
}

main(args).then(() => {
    process.exit(0);
}).catch(err => {
    console.error(err);
    process.exit(1);
});