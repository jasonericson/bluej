import neo4j from 'neo4j-driver'
import { QueryResult, Record } from 'neo4j-driver';
import { BskyAgent, AtpSessionEvent, AtpSessionData } from '@atproto/api'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { parallelQueries } from './parralel-queries'
import { weightedRoundRobin, deduplicateArray } from './weighted-round-robin'
import { followSimpleQuery } from './queries'

export const shortname = 'chaos'

export const uri = 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.generator/chaos'

interface LastSeed {
    seed: number;
    timestamp: number;
}
let userLastSeeds: Array<LastSeed> = []

interface PostData {
    randId: number;
    uri: string;
    repostUri: string;
}

// check the userLastSeeds array every 60 minutes and remove any entry that is older than 12 hours
setInterval(() => {
    const currentTime = Date.now()
    const twelveHoursAgo = currentTime - (12 * 60 * 60 * 1000)
    userLastSeeds = userLastSeeds.filter(lastSeed => lastSeed.timestamp > twelveHoursAgo)
}, 60 * 60 * 1000)

function xorshift(value: number): number {
    // Xorshift*32
    // Based on George Marsaglia's work: http://www.jstatsoft.org/v08/i14/paper
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;
    return value;
}

function getSafeSeed(seed: number): number {
    if (seed === 0) return 1;
    return seed;
}

// gets a hash code from the CID string
function hashCode(str: string, seed: number): number {
    let hash = 0;
    if (str) {
        const l = str.length;
        // iterate through CID in reverse, cause CIDs that start similarly (aka CIDs that have been posted close to each other)
        //   have too similar hash code results. this provides more variety
        for (let i = l - 1; i >= 0; i--) {
            hash = (hash << 5) - hash + str.charCodeAt(i) + seed;
            hash |= 0;
            hash = xorshift(hash);
        }
    }
    return getSafeSeed(hash);
}

// This is the main handler for the feed generator
export const handler = async (ctx: AppContext, params: QueryParams, requesterDid: string) => {
    try {
        const driver = neo4j.driver(
            'bolt://localhost',
            neo4j.auth.basic('', '')
        );
        const agent = new BskyAgent({
            service: 'https://bsky.social/'
        })

        if (requesterDid.length === 0) {
            console.log('[ERROR] requesterDid is empty, using default')
            requesterDid = 'did:plc:da4qrww7zq3flsr2zialldef'
        }

        let limit = params.limit ? params.limit : 50
        let cursor = params.cursor ? params.cursor : undefined

        let position: number = 0  // the return array will be sliced from this position to the limit, default is 0 if no cursor is provided
        let randId: number = Number.MIN_SAFE_INTEGER  // we'll use this to track where we are in the list from the cursor

        if (cursor !== undefined) {
            const [randIdCur, did] = cursor.split('::')
            randId = parseInt(randIdCur)
            if (!randId || !did || isNaN(randId)) {
                console.log('[ERROR] malformed cursor ', cursor)
                throw new InvalidRequestError('[ERROR] malformed cursor ', cursor)
            }
            if (did !== requesterDid) {
                console.log('[ERROR] JWT and cursor DID do not match', cursor)
                throw new InvalidRequestError('[ERROR] JWT and cursor DID do not match', cursor)
            }
        }
        // define some sane limits to avoid abuse
        limit = limit < 600 ? limit : 600

        console.log('[chaos] [', requesterDid, '] l:', limit, 'c:', cursor)

        // if necessary, populate the requester's following list
        // (for any people they followed before this feed service started running)
        let session = driver.session()
        let followsPrimedResult = await session.run('MATCH (requester:Person { did:$requesterDid }) RETURN requester.follows_primed', { requesterDid: requesterDid })
        if (followsPrimedResult.records.length === 0 || followsPrimedResult.records[0].get(0) !== true) {
            console.log("priming follows")
            await agent.login({ identifier: <string>process.env.FEEDGEN_HANDLE, password: <string>process.env.FEEDGEN_PASSWORD })

            let followCount = 0
            let follows = await agent.getFollows({ actor: requesterDid, limit: 100})
            let cursor = follows.data.cursor
            do {
                for (const follow of follows.data.follows) {
                    await session.run("MERGE (p1:Person {did: $requesterDid}) MERGE (p2:Person {did: $subjectDid}) ON CREATE SET p2.follows_primed = false MERGE (p1)-[:FOLLOW]->(p2)", { requesterDid: requesterDid, subjectDid: follow.did })
                }
                cursor = follows.data.cursor
                followCount += follows.data.follows.length
                follows = await agent.getFollows({ actor: requesterDid, limit: 100, cursor: cursor})
            } while (cursor !== undefined)

            await session.run("MATCH (p:Person {did: $requesterDid}) SET p.follows_primed = true", { requesterDid: requesterDid })

            console.log('follow count: ' + followCount)
        }

        // the number of results defined by limit determines how it will be distributed in the weightedRoundRobin call below
        let queryResult = await session.run(followSimpleQuery, { did: requesterDid })

        console.log('[chaos] follow results: ', queryResult.records.length)

        // get the sorting seed
        let seed = 0
        if (userLastSeeds[requesterDid] !== undefined && userLastSeeds[requesterDid].seed !== undefined) {
            seed = userLastSeeds[requesterDid].seed
        }

        // an undefined cursor and larger limit indicates a full refresh, so we'll update the seed to reorder everything
        if (cursor === undefined && limit > 20) {
            seed += 1
            userLastSeeds[requesterDid] = {
                seed: seed,
                timestamp: Date.now()
            }
        }

        // give each post a randId, statically hashed from its CID
        let posts: PostData[] = []
        for (let i = 0; i < queryResult.records.length; i++) {
            posts.push({
                randId: hashCode(queryResult.records[i].get(2), seed),
                uri: queryResult.records[i].get(1),
                repostUri: queryResult.records[i].get(3),
            })
        }

        // sort posts by randId
        posts.sort((a, b) => {
            if (a.randId < b.randId) {
                return -1
            } else if (b.randId < a.randId) {
                return 1
            } else {
                return 0
            }
        })

        // remove any reposts that are already here as regular posts
        let toRemove: PostData[] = []
        for (let i = 0; i < posts.length; i++) {
            if (posts[i].repostUri !== null) continue;
            for (let j = 0; j < posts.length; j++) {
                if (posts[i].uri == posts[j].repostUri) {
                    toRemove.push(posts[j])
                }
            }
        }
        for (let i = 0; i < toRemove.length; i++) {
            const remove = toRemove[i]
            posts.splice(posts.indexOf(remove), 1);
        }

        // remove duplicate reposts (keep the first one in the list)
        for (let i = 0; i < posts.length; i++) {
            if (posts[i].repostUri === null) continue;
            let j = i + 1
            while (j < posts.length) {
                if (posts[i].repostUri == posts[j].repostUri) {
                    posts.splice(j, 1)
                } else {
                    j++
                }
            }
        }

        // find position based on randId from cursor
        for (position = 0; position < posts.length; position++)
        {
            if (posts[position].randId > randId) {
                break
            }
        }

        // slice array from position to limit
        posts = posts.slice(position, position + limit)
        if (posts.length > 0) randId = posts[posts.length - 1].randId

        cursor = encodeURI(randId + '::' + requesterDid)

        // The feed format contains an array of post: uri, so map it to just this field
        const feed = posts.map((row) => {
            if (row.repostUri !== null && row.repostUri !== undefined) {
                return {
                    post: row.repostUri,
                    reason: {
                        $type: "app.bsky.feed.defs#skeletonReasonRepost",
                        repost: row.uri,
                    }
                }
            } else {
                return {
                    post: row.uri
                }
            }
        })

        return {
            cursor,
            feed,
        }

    } catch (e) {
        console.error(e)
    }
    return { feed: [] }
}
