{-# LANGUAGE
    ParallelListComp
  , MonadComprehensions
  #-}
-- | Generic combinators and transformations for queries
module Vaultaire.Query.Combinators
       ( aggregate, aggregateQ, takeQ, dropQ, takeWhileQ, dropWhileQ, cacheQ, maybeQ, foreachQ)
where

import           Control.Monad.Error
import qualified Data.Map as M
import           Data.Map (Map)
import           Pipes
import qualified Pipes.Prelude as P

import           Vaultaire.Query.Base

-- | Runs an aggregation function over the result of a query.
aggregate :: (Producer a m () -> m x) -> Query m a -> m x
aggregate f (Select l) = f l

-- | Same as 'aggregate' but returns a singleton query.
aggregateQ :: Monad m => (Producer a m () -> m x) -> Query m a -> Query m x
aggregateQ f = lift . aggregate f

takeQ :: Monad m => Int -> Query m a -> Query m a
takeQ n l = Select (every l >-> P.take n)

dropQ :: Monad m => Int -> Query m a -> Query m a
dropQ n l = Select (every l >-> P.drop n)

takeWhileQ :: Monad m => (a -> Bool) -> Query m a -> Query m a
takeWhileQ p l = Select (every l >-> P.takeWhile p)

dropWhileQ :: Monad m => (a -> Bool) -> Query m a -> Query m a
dropWhileQ p l = Select (every l >-> P.takeWhile p)

-- | Runs and cache the result of an (a, b) query in a map
cacheQ :: (Monad m, Ord a) => Query m (a, b) -> Query m (Map a b)
cacheQ = aggregateQ (P.fold insert' M.empty id)
  where insert' m (x,y) = M.insert x y m

maybeQ :: Monad m => Maybe a -> Query m a
maybeQ = Select . each

foreachQ :: (Monad m, Foldable f) => f a -> Query m a
foreachQ = Select . each
