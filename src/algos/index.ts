import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import * as Chaos from './chaos'

type AlgoHandler = (ctx: AppContext, params: QueryParams, requesterDid: string) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
  [Chaos.shortname]: Chaos.handler,
}

export default algos
