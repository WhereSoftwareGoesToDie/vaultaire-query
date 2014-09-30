--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--
{-# LANGUAGE TupleSections, StandaloneDeriving, GeneralizedNewtypeDeriving #-}

import           Control.Applicative
import           Control.Monad.Identity
import           Data.Function
import           Data.List
import qualified Data.Map as M
import           Data.Time.Calendar
import           Data.Time.Clock
import           Pipes
import qualified Pipes.Prelude as P
import           Test.Hspec
import           Test.QuickCheck

import           Vaultaire.Query
import           Vaultaire.Types
import           Marquise.Types


-- Pretty bad random time series generation here.

deriving instance Arbitrary Day

instance Arbitrary UTCTime where
  arbitrary =   UTCTime
            <$> arbitrary
            <*> (secondsToDiffTime <$> arbitrary)

instance Arbitrary SimplePoint where
  arbitrary =   SimplePoint
            <$> arbitrary
            <*> (convertToTimeStamp <$> arbitrary)
            <*> arbitrary

main :: IO ()
main = hspec $ do
  describe "combinator: cacheQ" $
    it "caches the results of a (key,val) query" $ property queryCache

  describe "query: fit time series - given two time series, assign for every point in the first series a range from the second series such that the point can be linearly interpolated in that range" $
    it "fits optimally" $ property fitOptimal

series :: Gen ([SimplePoint], [SimplePoint])
series = (,) <$> arbitrary <*> arbitrary

queryCache :: [Int] -> [Char] -> Bool
queryCache keys values =
  let query  = foreach $ zip (nub keys) values
      cached = head $ P.toList $ enumerate $ cacheQ query
      actual =        P.toList $ enumerate          query
  in  M.toList cached == (sortBy (compare `on` fst) actual)

fitOptimal :: [SimplePoint] -> [SimplePoint] -> Property
fitOptimal points other = (length points >= 2) && (length other >= 2) ==>
  let fitted = P.toList $ enumerate $ fit (mkQuery points) (mkQuery other)
  -- no one can do better!
  in  all (not . canFitBetter) fitted
  where mkQuery :: [SimplePoint] -> Query Identity SimplePoint
        mkQuery x = foreach (sortBy (compare `on` simpleTime) x)
        narrowerRanges (x1, x2) = [ (t1, t2)
                                  | t1 <- other , t2 <- other
                                  ,  simpleTime t1 > simpleTime x1
                                  && simpleTime t2 < simpleTime x2 ]
        canFit x (t1, t2) =  simpleTime t1 <= simpleTime x
                          && simpleTime x <= simpleTime t2
        canFitBetter (range, x) = any (canFit x) $ narrowerRanges range
