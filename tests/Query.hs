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
import           Control.Monad.State.Lazy
import           Data.Function
import           Data.List
import qualified Data.Map as M
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.Word
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

-- Address, Current Time, Time delta
newtype VaultGen a = VaultGen { unVaultGen :: State (Address, Word64, Word64) a }
    deriving (Functor, Applicative, Monad, MonadState (Address, Word64, Word64))

nextPoint :: Word64 -> VaultGen SimplePoint
nextPoint p = do
    (addr, oldTime, dT) <- get
    let newTime = oldTime + dT
    put (addr, newTime, dT)
    return $ SimplePoint addr (TimeStamp oldTime) p

nextPoints :: [Word64] -> VaultGen [SimplePoint]
nextPoints = mapM nextPoint

listToPoints :: Address -> Word64 -> Word64 -> [Word64] -> [SimplePoint]
listToPoints addr startTime dT ps = evalState (unVaultGen $ nextPoints ps) (addr, startTime, dT)

listToQuery :: Monad m => Address -> Word64 -> Word64 -> [Word64] -> Query m SimplePoint
listToQuery addr startTime dT ps = foreach $ listToPoints addr startTime dT ps

tenSec :: Word64
tenSec = 10*10^9

cumulTestPoints1, cumulTestPoints2 :: Monad m => Query m SimplePoint
cumulTestExpected1, cumulTestExpected2 :: Word64

cumulTestPoints1   = listToQuery (Address 17) 0 tenSec [20, 30, 10, 20, 30, 40, 10]
cumulTestExpected1 = 60

cumulTestPoints2   = listToQuery (Address 42) 0 tenSec [30, 40, 50, 60]
cumulTestExpected2 = 30


cumulTestPoints3   = listToQuery (Address 42) 0 tenSec [40, 30, 20, 10]
cumulTestExpected3 = 60

main :: IO ()
main = hspec $ do
  describe "combinator: cacheQ" $
    it "caches the results of a (key,val) query" $ property queryCache

  describe "query: fit time series - given two time series, assign for every point in the first series a range from the second series such that the point can be linearly interpolated in that range" $
    it "fits optimally" $ property fitOptimal
  describe "aggregateCumulativePoints: correctly aggregates cumulative points" $ do
    it "deals with multiple peaks" $ property testCumul1
    it "deals with no peaks" $ property testCumul2
    it "deals with constantly decreasing points" $ property testCumul3

testCumul1, testCumul2, testCumul3 :: Bool
testCumul1 = cumulTestExpected1 == runIdentity (aggregateCumulativePoints cumulTestPoints1)
testCumul2 = cumulTestExpected2 == runIdentity (aggregateCumulativePoints cumulTestPoints2)
testCumul3 = cumulTestExpected3 == runIdentity (aggregateCumulativePoints cumulTestPoints3)

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
