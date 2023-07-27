import neo4j from 'neo4j-driver'
import { QueryResult, Record } from 'neo4j-driver';
import { BskyAgent, AtpSessionEvent, AtpSessionData } from '@atproto/api'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { parallelQueries } from './parralel-queries'
import { weightedRoundRobin, deduplicateArray } from './weighted-round-robin'
import { postsFromTopEightQuery } from './queries'

export const shortname = 'favorites'

interface PostData {
    uri: string;
    repostUri: string;
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

        if (cursor !== undefined) {
            const [cid, positionCur] = cursor.split('::')            
            position = parseInt(positionCur)
            if (!cid || !position || isNaN(position) || position < 0) {
                console.log('[ERROR] malformed cursor ', cursor)
                throw new InvalidRequestError('[ERROR] malformed cursor ', cursor)
            }
            if (cid !== requesterDid) {
                console.log('[ERROR] JWT and cursor DID do not match', cursor)
                throw new InvalidRequestError('[ERROR] JWT and cursor DID do not match', cursor)
            }
        }
        // define some sane limits to avoid abuse
        position = position < 600 ? position : 600
        limit = limit < 600 ? limit : 600

        console.log('[favorites] [', requesterDid, '] l:', limit, 'c:', cursor)

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
        let queryResult = await session.run(postsFromTopEightQuery, { did: requesterDid })

        let posts: PostData[] = []
        for (let i = 0; i < queryResult.records.length; i++) {
            posts.push({
                uri: queryResult.records[i].get(1),
                repostUri: queryResult.records[i].get(3),
            })
        }

        let maxLength = posts.length
        posts = posts.slice(position, position + limit)

        position += limit

        if (maxLength > position + limit + 1) {
            cursor = encodeURI(requesterDid + '::' + position)
        }

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
