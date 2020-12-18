#!/usr/bin/env ts-node
"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var diffusion = require("diffusion");
var Common_1 = require("./Common");
// List the topics on a remove server
var BATCH_SIZE = 4096;
var args = process.argv.slice(2);
function buildPath(prefix, path) {
    return prefix ? prefix + '/' + path : path;
}
function listTopics(session, path) {
    return __awaiter(this, void 0, void 0, function () {
        var topicCount, request, jsonTopicSpec, selector, response, topicResults, _i, topicResults_1, r, lastTopicResult;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    topicCount = 0;
                    request = session.fetchRequest()
                        .first(BATCH_SIZE)
                        .withProperties();
                    jsonTopicSpec = new diffusion.topics.TopicSpecification(diffusion.topics.TopicType.JSON);
                    selector = Common_1.pathToSelector(path);
                    _a.label = 1;
                case 1: return [4 /*yield*/, request.fetch(selector)];
                case 2:
                    response = _a.sent();
                    topicResults = response.results();
                    for (_i = 0, topicResults_1 = topicResults; _i < topicResults_1.length; _i++) {
                        r = topicResults_1[_i];
                        console.log(r.path(), r.specification().properties);
                    }
                    topicCount += topicResults.length;
                    if (!response.hasMore()) {
                        return [3 /*break*/, 6];
                    }
                    if (!response.hasMore()) return [3 /*break*/, 4];
                    lastTopicResult = topicResults[topicResults.length - 1];
                    request = request.after(lastTopicResult.path());
                    return [4 /*yield*/, Common_1.pause(1000)];
                case 3:
                    _a.sent();
                    return [3 /*break*/, 5];
                case 4: return [3 /*break*/, 6];
                case 5:
                    if (true) return [3 /*break*/, 1];
                    _a.label = 6;
                case 6:
                    console.log("Total of " + topicCount + " topics");
                    return [2 /*return*/];
            }
        });
    });
}
function main(args) {
    return __awaiter(this, void 0, void 0, function () {
        var url, session;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (args.length < 1) {
                        console.error("wrong # args, try url");
                        process.exit(1);
                    }
                    url = new URL(args[0]);
                    return [4 /*yield*/, Common_1.buildSession(url)];
                case 1:
                    session = _a.sent();
                    // Start the great copy
                    return [4 /*yield*/, listTopics(session, url.pathname)];
                case 2:
                    // Start the great copy
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
main(args).then(function () {
    process.exit(0);
})["catch"](function (err) {
    console.error(err);
    process.exit(1);
});
