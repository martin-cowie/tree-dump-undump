#!/usr/bin/env ts-node
import * as diffusion from "diffusion";
import path from 'path';
import {buildSession} from './Common';
import lineByLine from 'n-readlines';
import { readFileSync } from "fs";
import ProgressBar from 'progress';

const BATCH_SIZE = 1024;
const args = process.argv.slice(2);

const jsonTopicSpec = new diffusion.topics.TopicSpecification(
    diffusion.topics.TopicType.JSON
);

function parseLine(line: string): any[] {
    return JSON.parse('[' + line + ']');
}

function mapPath(pathname: string, topicPath: string): string {

    if (pathname === '' || pathname === '/') {
        return topicPath;
    }
    return path.join(pathname.slice(1), topicPath);
}

function countLines(filename: string): number {
    const lines = new lineByLine(filename);
    let result = 0;
    while(lines.next()) {
        result ++;
    }
    return result;
}

async function main(args: string[]) {
    if (args.length < 2) {
        console.error(`wrong # args, try url dump-dir`);
        process.exit(1);
    }
    const url = new URL(args[0]);
    const dumpDir = args[1];
    const session = await buildSession(url);

    const topiclistFile = path.join(dumpDir, "topics.csv");
    const lineCount = countLines(topiclistFile)
    const lines = new lineByLine(topiclistFile);

    let line: Buffer | false;
    lines.next(); // Skip header

    var bar = new ProgressBar('  undumping [:bar] :rate topics/s :percent ETA :etas', {
        complete: '=',
        incomplete: ' ',
        width: 40,
        total: lineCount
      });
    bar.tick(0);


    let topicCount = 0;
    // let promises = [];
    while (line = lines.next()) {
        const [topicPath, type, size, hash, propStr, rfilename] = parseLine(line.toString());

        if (type === 'JSON') {
            const topicValue = JSON.parse(readFileSync(path.join(dumpDir, rfilename)).toString());
            await session.topicUpdate.set(mapPath(url.pathname, topicPath),
                diffusion.datatypes.json(),
                topicValue, {
                    specification: jsonTopicSpec
                }
            );
        } else {
            console.log(`Skipping ${topicPath}`);
        }
        bar.tick();
        topicCount++;

        // if (promises.length > 1024) {
        //     // bar.interrupt('syncing');
        //     console.log(`syncing`);

        //     // Hit type checker with big hammer
        //     await Promise.all(promises as unknown as Array<Promise<void>>);

        //     console.log(`syncing complete`);
        //     promises = [];
        // }
    }
    bar.terminate();
    console.log(`Created ${topicCount} topics on ${url}`);

}

main(args).then(() => {
    process.exit(0);
}).catch(err => {
    console.error(err);
    process.exit(1);
});