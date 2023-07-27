import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import * as Chaos from './chaos'
import * as Favorites from './favorites'

type AlgoHandler = (ctx: AppContext, params: QueryParams, requesterDid: string) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
  [Chaos.shortname]: Chaos.handler,
  [Favorites.shortname]: Favorites.handler,
}

export default algos
