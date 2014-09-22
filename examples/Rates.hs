{-# LANGUAGE TransformListComp, MonadComprehensions, TypeOperators, FlexibleContexts, ConstraintKinds #-}
module Rates where

import           Control.Monad.Trans.Reader
import qualified Data.ByteString.Char8 as B
import           Data.Word
import           Data.Csv
import           Data.Binary.IEEE754
import           Pipes.Safe
import qualified Pipes.Csv        as P

import           Vaultaire.Types
import           Marquise.Types
import           Vaultaire.Query
import           Vaultaire.Control.Lift

newtype Rate = Rate { unRate :: Word64 } deriving (Show)

instance P.ToField Rate where
  toField = B.pack . show . wordToDouble . unRate

instance P.ToField TimeStamp where
  toField = B.pack . show . (`div` 1000000000) . unTimeStamp

type HostName = String
type RespCode = String
data RespRates = RespRates { _code :: RespCode
                           , _host :: HostName
                           , _resppoint :: SimplePoint }
     deriving Show
data CpuRates = CpuRates  { _cpumetric :: String
                          , _cpupoint :: SimplePoint }
     deriving Show

instance P.ToRecord RespRates where
  toRecord (RespRates r host p) = record [ P.toField r, P.toField host, P.toField $ simpleTime p, P.toField $ Rate $ simplePayload p]

instance P.ToRecord CpuRates where
  toRecord (CpuRates metric p) = record [ P.toField metric, P.toField $ simpleTime p, P.toField $ Rate $ simplePayload p]

respRates :: ( ReaderT MarquiseContents `In` m
             , ReaderT MarquiseReader `In` m
             , MonadSafe m)
          => Origin -> TimeStamp -> TimeStamp
          -> Query m RespRates
respRates origin start end
  = [ RespRates respCode host p
    | (addr, dict) <- addressesAny origin [nginxErrorMetadata]
    , host         <- lookupQ "host" dict
    , respCode     <- lookupQ "metric" dict
    , p            <- metrics origin addr start end
    ]
    where nginxErrorMetadata = ("service", "nginx-error-rates")

cpuRates :: ( ReaderT MarquiseContents `In` m
            , ReaderT MarquiseReader `In` m
            , MonadSafe m)
          => Origin -> TimeStamp -> TimeStamp
          -> Query m CpuRates
cpuRates origin start end
  = [ CpuRates cpuMetric p
    | (addr, dict) <- addressesAll origin [cpuMetadata, hosts]
    , cpuMetric    <- lookupQ "metric" dict
    , p            <- metrics origin addr start end
    ]
    where cpuMetadata = ("service", "cpu")
          -- hosts = ("host", "bravo140")
          hosts = ("host", "fe3.prod.as.ratecity.com.au")

data Product = Product { resp :: SimplePoint
                       , cpu  :: SimplePoint
                       , respcode :: RespCode
                       , hostname :: HostName
                       , cpumetric :: String }

class Interpolatable a where
   inter :: a -> a -> Bool
   point :: a -> SimplePoint

instance Interpolatable CpuRates where
  inter (CpuRates n _) (CpuRates n' _) = n == n'
  point (CpuRates _ x) = x

instance Interpolatable RespRates where
  inter (RespRates c h _) (RespRates c' h' _) = c == c' && h == h'
  point (RespRates _ _ x) = x

time' :: Interpolatable a => a -> TimeStamp
time' p = simpleTime $ point p

val' :: Interpolatable a => a -> Double
val' p  = wordToDouble $ simplePayload $ point p

interpolateAt :: Interpolatable a => (SimplePoint -> a) -> [TimeStamp] -> [a] -> [a]
interpolateAt f ts@(t:times) (ts1:ts2:tss)
  -- time is between first two time points in the tss list
  | time' ts1 <= t && t < time' ts2 =
        [interpolate f t ts1 ts2] ++ interpolateAt f times tss
  -- We need to go farther into ts1:ts2:tss to find our time
  | otherwise =
      interpolateAt f ts (ts2:tss)
interpolateAt _ _ _ = []

interpolate :: Interpolatable a => (SimplePoint -> a) -> TimeStamp -> a -> a -> a
interpolate f t p1 p2
    | time' p1 == time' p2 = f (SimplePoint 0 t (doubleToWord ((val' p1 + val' p2) / 2)))
    | otherwise =
      let m = (val' p2 - val' p1) / fromIntegral (time' p2 - time' p1)
          b = val' p1
      in f (SimplePoint 0 t (doubleToWord (m*(fromIntegral (t - time' p1)) + b)))
