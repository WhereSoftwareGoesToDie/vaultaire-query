{-# LANGUAGE
    FlexibleContexts
  , TypeOperators
  , ParallelListComp
  , MonadComprehensions
  , TupleSections
  #-}
-- | Analytics queries on Vaultaire data.
module Query
       ( Query
       , module Query.Combinators
       , module Query.Connection
         -- * Analytics Queries
       , addresses, addressesAny, addressesAll, metrics, lookupQ, sumPoints, fitWith, fit
         -- * Helpful Predicates for Transforming Queries
       , fuzzy, fuzzyAny, fuzzyAll
       )
where

import           Control.Monad
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.State.Strict
import           Control.Lens (view)
import           Data.Word
import           Pipes
import           Pipes.Lift
import qualified Pipes.Prelude              as P
import qualified Pipes.Parse                as P
import qualified Data.Text                  as T

import           Vaultaire.Types
import           Marquise.Types
import           Marquise.Client (decodeSimple)

import           Query.Base
import           Query.Combinators
import           Query.Connection

-- Combinators specific to vaultaire types -------------------------------------

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
        -> Pipe (b, b)                    -- ^ range for which we will yield interpolateable times
                ((b, b), a)               -- ^ range and a time from the underlying producer
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

fit :: Monad m
    => Query m SimplePoint
    -> Query m SimplePoint
    -> Query m ((SimplePoint, SimplePoint), SimplePoint)
fit = fitWith interpolateable
  where interpolateable (p1, p2) p =  simpleTime p1 <= simpleTime p
                                   && simpleTime p  <= simpleTime p2

-- | Sum the value (payload) of a series of simple data points.
sumPoints :: Monad m => Query m SimplePoint -> Query m Word64
sumPoints = aggregateQ (\p -> P.sum (p >-> P.map simplePayload))

-- | Lookup a metadata key.
lookupQ :: Monad m
        => String         -- ^ key
        -> SourceDict     -- ^ metadata map
        -> Query m String -- ^ result as a query
lookupQ s d = [ T.unpack x | x <- maybeQ $ lookupSource (T.pack s) d ]


-- Built-in Marquise Queries ---------------------------------------------------

-- | All addresses (and their metadata) from an origin.
addresses :: (ReaderT MarquiseContents `In` m, MonadIO m)
          => Origin
          -> Query m (Address, SourceDict) -- ^ result address and its metadata map
addresses origin = Select $ do
  c <- liftT ask
  hoist liftIO $ enumerateAddresses c origin

-- | Addresses whose metadata match (fuzzily) any in a set of metadata key-values.
--   e.g. @addressesAny origin [("nginx", "error-rates"), ("metric", "cpu")]@
addressesAny :: (ReaderT MarquiseContents `In` m, MonadIO m)
             => Origin
             -> [(String, String)]            -- ^ metadata key-value constraints (fuzzy on values)
             -> Query m (Address, SourceDict) -- ^ result address and its metadata map
addressesAny origin mds
 = [ (addr, sd)
   | (addr, sd) <- addresses origin
   , or $ map (fuzzy sd) mds
   ]

-- | Addresses whose metadata match (fuzzily) all in a set of metadata key-values.
addressesAll :: (ReaderT MarquiseContents `In` m, MonadIO m)
             => Origin
             -> [(String, String)]            -- ^ metadata key-value constraints (fuzzy on values)
             -> Query m (Address, SourceDict) -- ^ result address and its metadata map
addressesAll origin mds
 = [ (addr, sd)
   | (addr, sd) <- addresses origin
   , and $ map (fuzzy sd) mds
   ]

-- | Data points for an address over some period of time.
metrics :: (ReaderT MarquiseReader `In` m, MonadIO m)
            => Origin
            -> Address
            -> TimeStamp           -- ^ start
            -> TimeStamp           -- ^ end
            -> Query m SimplePoint -- ^ result data point
metrics origin addr start end = Select $ do
  c <- liftT ask
  hoist liftIO $ readSimple c addr start end origin >-> decodeSimple


-- Helpers ---------------------------------------------------------------------

fuzzy :: SourceDict -> (String, String) -> Bool
fuzzy sd (k,v) = case lookupSource (T.pack k) sd of
  Just a  -> T.pack v `T.isInfixOf` a
  Nothing -> False

fuzzyAny :: SourceDict -> [(String, String)] -> Bool
fuzzyAny sd = or . map (fuzzy sd)

fuzzyAll :: SourceDict -> [(String, String)] -> Bool
fuzzyAll sd = and . map (fuzzy sd)
