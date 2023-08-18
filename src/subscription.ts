import { QueryResult, Record, Driver, Session } from 'neo4j-driver'
import { OutputSchema as RepoEvent, isCommit } from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { Database } from './db'
const neo4j = require('neo4j-driver')
import { clearOldPostsQuery } from './algos/queries'

const verbose = false
const outputError = false

interface RetryableQuery {
  query: string;
  params: object;
  retryCount: number;
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private driver: Driver;
  private queryQueue: RetryableQuery[];
  private intervalId?: NodeJS.Timeout;
  private clearIntervalId?: NodeJS.Timeout;
  private lastClearTime: number
  private lastHour: number

  constructor(public db: Database, public service: string) {
    super(db, service)
    this.driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("", ""), {
      encrypted: 'ENCRYPTION_OFF'
    })
    this.queryQueue = [];
    this.intervalId = undefined;
    this.startProcessingQueue()

    this.lastClearTime = new Date().getTime()
    this.lastHour = 0
  }

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)
    if (ops.posts.deletes.length > 0) {
      for (const post of ops.posts.deletes) {
        if (verbose) process.stdout.write('P')
        try {
          await this.executeQuery("MATCH (p:Post {uri: $uri}) DETACH DELETE p", {
            uri: post.uri
          })
        } catch (err) {
          if (outputError) console.error('[ERROR POST DELETE]:', err)
        }
      }
    }
    if (ops.posts.creates.length > 0) {
      for (const post of ops.posts.creates) {
        if (verbose) process.stdout.write('p')
        await this.executeQuery(`
          CREATE (post:Post {uri: $uri, cid: $cid, author: $author, text: $text, createdAt: $createdAt, indexedAt: LocalDateTime()})
          MERGE (person:Person {did: $author})
          ON CREATE SET person.follows_primed = false
          MERGE (person)-[:AUTHOR_OF]->(post)`, {
          uri: post.uri,
          cid: post.cid,
          author: post.author,
          text: post.record.text,
          createdAt: post.record.createdAt
        })
        const replyRoot = post.record?.reply?.root ? post.record.reply.root.uri : null
        const replyParent = post.record?.reply?.parent ? post.record.reply.parent.uri : null
        if (replyRoot) {
          await this.executeQuery(`
            MERGE (post1:Post {uri: $uri})
            MERGE (post2:Post {uri: $rootUri})
            MERGE (post1)-[:ROOT]->(post2)`, {
            uri: post.uri,
            rootUri: replyRoot
          })
        }
        if (replyParent) {
          await this.executeQuery(`
            MERGE (post1:Post {uri: $uri})
            MERGE (post2:Post {uri: $parentUri})
            MERGE (post1)-[:PARENT]->(post2)`, {
            uri: post.uri,
            parentUri: replyParent,
          })
          // record interaction
          await this.executeQuery(`
            MATCH (post2:Post {uri: $parentUri})
            MATCH (person2:Person)-[:AUTHOR_OF]->(post2)
            WHERE person2.did != $author
            MATCH (post1:Post {uri: $uri})
            MATCH (person1:Person {did: $author})
            MERGE (person1)-[i:INTERACTION]->(person2)
            ON CREATE SET i.likes = [0,0,0,0,0,0,0], i.replies = [1,0,0,0,0,0,0], i.reposts = [0,0,0,0,0,0,0]
            ON MATCH SET i.replies = [i.replies[0] + 1] + i.replies[1..7]`, {
            parentUri: replyParent,
            author: post.author,
            uri: post.uri,
          })
        }
      }
    }
    if (ops.follows.deletes.length > 0) {
      for (const follow of ops.follows.deletes) {
        if (verbose) process.stdout.write('F')
          const result = await this.executeQuery(`
            MATCH (f:FOLLOW {uri: $uri})
            DETACH DELETE f
          `, {
            uri: follow.uri
          })
      }
    }
    if (ops.follows.creates.length > 0) {
      for (const follow of ops.follows.creates) {
        if (verbose) process.stdout.write('f')
          await this.executeQuery(`
            MERGE (p1:Person {did: $authorDid})
            ON CREATE SET p1.follows_primed = false
            MERGE (p2:Person {did: $subjectDid})
            MERGE (p1)-[:FOLLOW {uri: $uri}]->(p2)
          `, {
            authorDid: follow.author,
            subjectDid: follow.record.subject,
            uri: follow.uri,
          })
      }
    }

    if (ops.likes.creates.length > 0) {
      for (const like of ops.likes.creates) {
        await this.executeQuery(`
          MATCH (p2:Person)-[:AUTHOR_OF]->(post:Post {uri: $postUri})
          WHERE p2.did != $authorDid
          MERGE (p1:Person {did: $authorDid})
          ON CREATE SET p1.follows_primed = false
          MERGE (p1)-[i:INTERACTION]->(p2)
          ON CREATE SET i.likes = [1,0,0,0,0,0,0], i.replies = [0,0,0,0,0,0,0], i.reposts = [0,0,0,0,0,0,0]
          ON MATCH SET i.likes = [i.likes[0] + 1] + i.likes[1..7]
        `, {
          postUri: like.record.subject.uri,
          authorDid: like.author,
        })
      }
    }

    // if (ops.likes.deletes.length > 0) {
    //   for (const like of ops.likes.deletes) {
    //     if (verbose) process.stdout.write('L')
    //     //FIXME sane situation as with follows.delete, just a URI and not a full source -> dest mapping
    //   }
    // }
    if (ops.reposts.deletes.length > 0) {
      for (const repost of ops.reposts.deletes) {
        if (verbose) process.stdout.write('R')
        await this.executeQuery("MATCH (p:Post {uri: $uri}) DETACH DELETE p", {
          uri: repost.uri
        })
      }
    }
    if (ops.reposts.creates.length > 0) {
      for (const repost of ops.reposts.creates) {
        if (verbose) process.stdout.write('r')
        await this.executeQuery(`
          CREATE (p:Post {uri: $uri, cid: $cid, author: $author, repostUri: $repostUri, createdAt: $createdAt, indexedAt: LocalDateTime()})
          MERGE (person:Person {did: $author})
          ON CREATE SET person.follows_primed = false
          MERGE (person)-[:AUTHOR_OF]->(p)
          RETURN p`, {
          uri: repost.uri,
          cid: repost.cid,
          author: repost.author,
          repostUri: repost.record.subject.uri,
          createdAt: repost.record.createdAt
        })
        // record interaction
        await this.executeQuery(`
          MATCH (ogPost:Post {uri: $repostUri})
          MATCH (ogAuthor:Person)-[:AUTHOR_OF]->(ogPost)
          WHERE ogAuthor.did != $author
          MATCH (rpAuthor:Person {did: $author})
          MERGE (rpAuthor)-[i:INTERACTION]->(ogAuthor)
          ON CREATE SET i.likes = [0,0,0,0,0,0,0], i.replies = [0,0,0,0,0,0,0], i.reposts = [1,0,0,0,0,0,0]
          ON MATCH SET i.reposts = [i.reposts[0] + 1] + i.reposts[1..7]
          `, {
            repostUri: repost.record.subject.uri,
            author: repost.author,
        })
      }
    }

    const today = new Date()
    const now = today.getTime()
    const timeSinceLastClear = now - this.lastClearTime
    if (timeSinceLastClear > 5 * 60 * 1000) {
      try {
        this.lastClearTime = now
        await this.executeQuery(clearOldPostsQuery)
        console.log("Ran query to clear old posts")
      } catch (err) {
        console.error('[ERROR POST DELETE]: ', err)
      }
    }

    // at midnight, shift all the link records over a day
    if (today.getHours() === 0 && this.lastHour !== 0) {
      try {
        await this.executeQuery(`
          MATCH (:Person)-[i:INTERACTION]->(:Person)
          SET i.likes = [0] + i.likes[0..6], i.replies = [0] + i.replies[0..6], i.reposts = [0] + i.reposts[0..6]
          `)
        console.log("Ran query to shift likes over a day")
      } catch (err) {
        console.error('[ERROR SHIFT DAYS]: ', err)
      }
    }
    this.lastHour = today.getHours()
  }

  async executeQuery(query: string, params: object = {}, retryCount: number = 10): Promise<void> {
    const session = this.driver.session()
    try {
      await session.run(query, params);
    } catch (error) {
      if (this.isRetryableError(error) && retryCount > 0) {
        this.queryQueue.push({
          query,
          params: params,
          retryCount: retryCount - 1
        })
        console.log('Query failed, retrying later: ', query)
      } else {
        let message = 'Unknown Error'
        if (error instanceof Error) message = error.message
        console.log('Query failed, giving up:', message, 'query: ', query)
      }
    } finally {
      session.close()
    }
  }

  private isRetryableError(error: any): boolean {
    // look at the exception isRetryable ?
    return true;
  }

  async processQueryQueue(): Promise<void> {
    const queueLength = this.queryQueue.length;
    for (let i = 0; i < queueLength; i++) {
      const {
        query,
        params,
        retryCount
      } = this.queryQueue.shift()!;
      this.executeQuery(query, params, retryCount);
    }
  }

  startProcessingQueue(intervalSeconds: number = 5): void {
    if (!this.intervalId) {
      this.intervalId = setInterval(() => {
        this.processQueryQueue();
      }, intervalSeconds * 1000);
    }
  }

  stopProcessingQueue(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
  }
}
