#!/usr/bin/env ts-node
import * as diffusion from "diffusion";
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import {mapType, pause, buildSession, ensureDir, getStats} from './Common';
import ProgressBar from 'progress';

const BATCH_SIZE = 1000;

function saveTopicContent(root:string, topicPath:string, content:string, type: diffusion.TopicType): string {

    // Map the topic path, to an acceptable filename
    const result = `${topicPath}.${mapType(type)}`;
    const fqFilename = path.join("dumps", root, result);
    const parentDir = path.dirname(fqFilename);

    ensureDir(parentDir)

    fs.writeFileSync(fqFilename, JSON.stringify(content, null, 4));
    return result;
}


function toCSV(r: any) {
    return JSON.stringify(r).slice(1, -1) + "\n";
}

async function main(args: string[]) {

    if (args.length < 1) {
        console.error(`wrong # args, try $0 url`);
        process.exit(1);
    }
    const url = new URL(args[0]);

    const session = await buildSession(url);
    console.error(`Batch size = ${BATCH_SIZE}`);

    let [estTopicCount, topicStats] = await getStats(session);
    console.error(`Dumping an estimated ${estTopicCount.toLocaleString()} topics, with breakdown`, topicStats);

    let request = await session.fetchRequest().first(BATCH_SIZE).withProperties().withValues(diffusion.datatypes.json());
    let topicCount = 0;

    const csvFilename = path.join("dumps", url.host, "topics.csv");
    ensureDir(path.dirname(csvFilename));
    const csvFileStream = fs.createWriteStream(csvFilename);
    csvFileStream.write(toCSV(['path', 'type', 'size', 'md5', 'properties', 'filename']));

    var bar = new ProgressBar('  dumping [:bar] :rate topics/s :percent ETA :etas', {
        complete: '=',
        incomplete: ' ',
        width: 40,
        total: estTopicCount
      });
    bar.tick(0);

    do {
        const response = await request.fetch('?.*//');
        const results = response.results();

        topicCount += results.length;

        results.forEach((r) => {
            //TODO: scope exists to record the CBOR size of JSON topics, via r.value().$buffer.length

            const value = r?.value()?.toString();
            const sum = crypto.createHash('md5');
            sum.update(value);
            const md5 = sum.digest('hex');

            const relFilename = saveTopicContent(url.host , r.path() as string, r.value().get(), r.type());
            const lineStr = toCSV([
                r.path(),
                mapType(r.type()),
                value.length,
                md5,
                JSON.stringify(r.specification().properties),
                relFilename
            ]);

            csvFileStream.write(lineStr);

        });
        const lastTopicResult = results[results.length -1];

        if (!response.hasMore()) {
            break;
        }

        bar.tick(results.length);
        request = request.after(lastTopicResult.path());
        await pause(1000);

    } while(true);
    csvFileStream.close();
    console.error(`Saved ${csvFilename}`);
    console.error(`Fetch ${topicCount} topics from ${url.host}`);
}

main(process.argv.slice(2)).catch(err => {
    console.error(err);
}).then(() => {
    process.exit(0);
});