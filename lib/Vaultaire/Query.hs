{-# LANGUAGE
    FlexibleContexts
  , TypeOperators
  , ParallelListComp
  , MonadComprehensions
  , TupleSections
  #-}
-- | Analytics queries on Vaultaire data.
module Vaultaire.Query
       ( Query
       , module Vaultaire.Query.Combinators
       , module Vaultaire.Query.Connection
         -- * Analytics Queries
       , addresses, addressesAny, addressesAll, addressesWith, metrics
       , eventMetrics, lookupQ, sumPoints
       , align
       , fitWith , fitSimple, aggregateCumulativePoints
         -- * Helpful Predicates for Transforming Queries
       , fuzzy, fuzzyAny, fuzzyAll
       )
where

import           Control.Monad
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.State.Strict
import           Control.Lens (view)
import           Data.Word
import           Data.Either
import qualified Data.Text                  as T
import           Data.Text.Encoding         (encodeUtf8)
import           Pipes
import           Pipes.Lift
import qualified Pipes.Prelude              as P
import qualified Pipes.Parse                as P
import           Pipes.Safe
import           Data.Maybe
import           Network.URI
import qualified System.ZMQ4                as Z
import           Prelude hiding (sum, last)

import           Vaultaire.Types
import           Marquise.Types
import qualified Marquise.Client            as M
import qualified Chevalier.Util             as C
import qualified Chevalier.Types            as C

import           Vaultaire.Query.Base
import           Vaultaire.Query.Combinators
import           Vaultaire.Query.Connection
import           Vaultaire.Control.Lift


-- Ranges ----------------------------------------------------------------------

spanPoints :: Monad m
           => ((b, b) -> a -> Bool)
           -> (b, b)                     -- ^ range that we are allowed to interpolate over
           -> Producer a m ()            -- ^ source of times that needs interpolating
           -> Producer a                 --   points from the above source that can be interpolated in this range
                       m
                       (Producer a m ()) --   the rest of the points (that cannot be interpolated in this range)
spanPoints interpolateable r = view $ P.span (interpolateable r)

inRange :: Monad m
        => ((b, b) -> a -> Bool)
        -> Pipe (b, b)                     -- ^ range for which we will yield interpolateable times
                ((b, b), a)                -- ^ range and a time from the underlying producer
                (StateT (Producer a m ())  -- ^ the underlying producer of points
                        m)
                ()
inRange f = forever $ do
  range   <- await
  points  <- lift get
  points' <- runEffect (for (hoist   (lift . lift)              -- lift the underlying monad @m@ of the spanPoints producer
                                   $ spanPoints f range points) --   into pipe over stateT land
                            (lift . yield . (range,)))          -- then yield all the points from the outer spanPoints producer
                                                                --   into the underlying monad (now a pipe stateT instead of `m`
                                                                --   because we have lifted)
  lift $ put points'                                            -- we get the "leftover" times (not interpolateable for this range)
                                                                --   back, so put that in the state.

-- | Given time series, e.g. @s1 = [ 0, 1, 2, 4, ... ]@ and @s2 = [ 0, 2, 5, ... ]@
--   assign for every time point in @s1@ a range from @s2@,
--   such that the point can be interpolated in that range.
--   e.g. @[ (0, (0,2)), (1, (0,2)), (2, (0,2)), (4, (2,5)) ... ]@
fitWith :: Monad m
        => ((b, b) -> a -> Bool)
        -> Query m a           -- ^ times at which we want to interpolate
        -> Query m b           -- ^ times on which we want to interpolate
        -> Query m ((b, b), a) -- ^ pair of a range from the second series
                               --   and a point from the first series that fits in said range
fitWith f points ranges = Select (enumerate (rangify ranges) >-> evalStateP (enumerate points) (inRange f))
  where rangify :: (Monad m) => Query m x -> Query m (x, x)
        rangify series = [ (x,y) | x <- series
                                 | y <- Select $ enumerate series >-> P.drop 1 ]

fitSimple :: Monad m
          => Query m SimplePoint
          -> Query m SimplePoint
          -> Query m ((SimplePoint, SimplePoint), SimplePoint)
fitSimple = fitWith interpolateable
  where interpolateable (p1, p2) p =  simpleTime p1 <= simpleTime p
                                   && simpleTime p  <= simpleTime p2


-- Alignment -------------------------------------------------------------------

barrier :: Monad m
        => Pipe SimplePoint                         -- ^ pass these points along, this is the "tortoise"
                SimplePoint                         -- ^ the above points, and additionally any points needed to align
                (StateT (Producer SimplePoint m ()) -- ^ barriers, this is the "hare"
                         m)
                ()
barrier = forever $ do
  x         <- await
  barriers  <- lift $ get
  barriers' <- go x barriers
  lift $ put barriers'
  where go x p = do
          b <- lift $ lift $ next p
          case b of
            Left   _ -> yield x >> return p
            Right (y, p') -> case compare (simpleTime y) (simpleTime x) of
              LT     -> yield (SimplePoint (simpleAddress x) (simpleTime y) (simplePayload x))
                                >> go x p'
              GT     -> yield x >> return p
              EQ     -> yield x >> return p'

-- | Align the first series to the times in the second series, e.g.
--   s1 = [     2     5 ]
--   s2 = [ 0 1 2   4 5 ]
--   result would be [ 0 1 2 4 5 ]
--
align :: Monad m
      => Query m SimplePoint
      -> Query m SimplePoint
      -> Query m SimplePoint
align (Select s1) (Select s2) = Select $ s1 >-> evalStateP s2 barrier

-- Aggregation -----------------------------------------------------------------

-- | Sum the value (payload) of a series of simple data points.
sumPoints :: Monad m => Query m SimplePoint -> Query m Word64
sumPoints = aggregateQ (\p -> P.sum (p >-> P.map simplePayload))

-- | Openstack cumulative data is from last startup.
--   So when we process cumulative data we need to account for this.
--   Since (excluding restarts) each point is strictly non-decreasing,
--   we simply use a modified fold to deal with the case where the latest point
--   is less than the second latest point (indicating a restart)
aggregateCumulativePoints :: Monad m => Query m SimplePoint -> m Word64
aggregateCumulativePoints (Select points) = do
    res <- next points
    case res of
        Left _ -> return 0
        Right (p, points') -> do
            let v = simplePayload p
            P.fold helper (0, v) (\(a, b) -> a + b - v) points'
  where
    helper (sum, last) (SimplePoint _ _ v) =
        if v < last
            then (sum+last, v)
            else (sum, v)

-- | Lookup a metadata key.
lookupQ :: Monad m
        => String         -- ^ key
        -> SourceDict     -- ^ metadata map
        -> Query m String -- ^ result as a query
lookupQ s d = [ T.unpack x | x <- maybeQ $ lookupSource (T.pack s) d ]


-- Primimtives -----------------------------------------------------------------

-- | All addresses (and their metadata) from an origin.
addresses :: MonadSafe m
          => URI
          -> Origin
          -> Query m (Address, SourceDict) -- ^ result address and its metadata map
addresses uri origin = Select $ enumerateOrigin uri origin

-- | Addresses whose metadata match (fuzzily) any in a set of metadata key-values.
--   e.g. @addressesAny origin [("nginx", "error-rates"), ("metric", "cpu")]@
addressesAny :: MonadSafe m
             => URI
             -> Origin
             -> [(String, String)]            -- ^ metadata key-value constraints (fuzzy on values)
             -> Query m (Address, SourceDict) -- ^ result address and its metadata map
addressesAny uri origin mds
 = [ (addr, sd)
   | (addr, sd) <- addresses uri origin
   , any (fuzzy sd) mds
   ]

-- | Addresses whose metadata match (fuzzily) all in a set of metadata key-values.
addressesAll :: MonadSafe m
             => URI
             -> Origin
             -> [(String, String)]            -- ^ metadata key-value constraints (fuzzy on values)
             -> Query m (Address, SourceDict) -- ^ result address and its metadata map
addressesAll uri origin mds
 = [ (addr, sd)
   | (addr, sd) <- addresses uri origin
   , all (fuzzy sd) mds
   ]

-- | Data points for an address over some period of time.
metrics :: MonadSafe m
        => URI
        -> Origin
        -> Address
        -> TimeStamp           -- ^ start
        -> TimeStamp           -- ^ end
        -> Query m SimplePoint -- ^ result data point
metrics uri origin addr start end = Select $ do
  readSimple uri addr start end origin >-> M.decodeSimple

-- | To construct event based data correctly we need to query over all time
eventMetrics :: MonadSafe m
             => URI
             -> Origin
             -> Address
             -> Query m SimplePoint -- ^ result data point
eventMetrics uri origin addr = Select $ do
  let start = TimeStamp 0
  end <- liftIO getCurrentTimeNanoseconds
  readSimple uri addr start end origin >-> M.decodeSimple

readSimple :: MonadSafe m
           => URI
           -> Address -> TimeStamp -> TimeStamp -> Origin
           -> Producer SimpleBurst m ()
readSimple uri a s e o = runMarquiseReader uri $ do
  (MarquiseReader c) <- lift ask
  hoist liftIO $ M.readSimple a s e o c

enumerateOrigin :: MonadSafe m
                   => URI
                   -> Origin
                   -> Producer (Address, SourceDict) m ()
enumerateOrigin uri o = runMarquiseContents uri $ do
  (MarquiseContents c) <- liftT ask
  hoist liftIO $ M.enumerateOrigin o c

-- Built-in Chevalier Queries --------------------------------------------------

addressesWith :: MonadSafe m
              => URI
              -> Origin
              -> C.SourceRequest
              -> Query m (Address, SourceDict)
addressesWith chev org request = runChevalier chev $ Select $ do
  c <- liftT ask
  hoist liftIO $ chevalier c org request

chevalier :: Chevalier -> Origin -> C.SourceRequest
          -> Producer (Address, SourceDict) IO ()
chevalier (Chevalier sock) origin request = do
  resp <- liftIO sendrecv
  -- this doesn't actually stream because chevalier doesn't
  each $ either (error . show) (rights . map C.convertSource) (C.decodeResponse resp)
  where sendrecv = do
          Z.send sock [Z.SendMore] $ encodeOrigin origin
          Z.send sock []           $ C.encodeRequest request
          Z.receive sock
        encodeOrigin (Origin x) = encodeUtf8 $ T.pack $ show x

-- Helpers ---------------------------------------------------------------------

fuzzy :: SourceDict -> (String, String) -> Bool
fuzzy sd (k,v) = case lookupSource (T.pack k) sd of
  Just a  -> T.pack v `T.isInfixOf` a
  Nothing -> False

fuzzyAny :: SourceDict -> [(String, String)] -> Bool
fuzzyAny sd = any (fuzzy sd)

fuzzyAll :: SourceDict -> [(String, String)] -> Bool
fuzzyAll sd = all (fuzzy sd)
