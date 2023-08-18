// Queries used by BlueJ that implement thex various algorithms for fetching posts from the graph.

export const followSimpleQuery = `
    MATCH (my_person:Person {did: $did })-[:FOLLOW]->(follow_person:Person)
    MATCH (follow_person) - [:AUTHOR_OF] -> (post:Post)
    WHERE post.indexedAt IS NOT NULL AND NOT exists((post)-[:PARENT]->(:Post)) AND (localDateTime() - post.indexedAt).second < 60 * 60 * 12
    RETURN ID(post), post.uri, post.cid, post.repostUri
    ORDER BY post.indexedAt DESC
    LIMIT 500
`

export const clearOldPostsQuery = `
    MATCH (post:Post)
    WITH localDateTime() - post.indexedAt as duration, post
    WHERE duration.second > 60 * 60 * 48
    DETACH DELETE post
`

export const postsFromTopEightQuery = `
    MATCH (p1:Person {did: $did })-[i:INTERACTION]->(p2:Person)
    WHERE exists((p1)-[:FOLLOW]->(p2))
    WITH reduce(totalLikes = 0, n in i.likes | totalLikes + n) * 1 + reduce(totalReposts = 0, n in i.reposts | totalReposts + n) * 2 + reduce(totalReplies = 0, n in i.replies | totalReplies + n) * 3 as score, p2
    ORDER BY score DESC
    LIMIT 8
    MATCH (p2)-[:AUTHOR_OF]->(post:Post)
    WHERE post.indexedAt IS NOT NULL AND NOT exists((post)-[:PARENT]->(:Post))
    RETURN ID(post), post.uri, post.cid, post.repostUri
    ORDER BY post.indexedAt DESC
    LIMIT 500
`
