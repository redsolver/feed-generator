import { Post } from './db/schema'
import { Record as PostRecord } from './lexicon/types/app/bsky/feed/post'
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType, CreateOp } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)

    function buildPost(create: CreateOp<PostRecord>, feed: string): Post {
      return {
        uri: create.uri,
        cid: create.cid,
        replyParent: create.record?.reply?.parent.uri ?? null,
        replyRoot: create.record?.reply?.root.uri ?? null,
        indexedAt: new Date().toISOString(),
        feed: feed,
      };
    }

    const httpPostsToCreate = ops.posts.creates
      .filter((create) => {
        return create.record.text.toLowerCase().includes('http')
      })
      .map((create) => {
        return buildPost(create, 'posts-with-links')
      })

    const eurovisionPostsToCreate = ops.posts.creates
      .filter((create) => {
        const text = create.record.text.toLowerCase();
        return text.includes('eurovision')
          || text.includes(' esc ')
          || text.includes('song')
          || text.includes('contest')
      })
      .map((create) => {
        return buildPost(create, 'eurovision')
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (httpPostsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(httpPostsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
    if (eurovisionPostsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(eurovisionPostsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
