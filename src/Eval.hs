{-# LANGUAGE DeriveGeneric, RecordWildCards, TupleSections #-}

module Eval where

import           Control.Applicative
import           Control.Monad
import           Data.Either.Combinators
import           Data.Maybe
import           Data.Monoid
import qualified Data.ByteString.Char8 as B8
import           Network.URI
import           Pipes
import qualified Pipes.ByteString as PB
import qualified Pipes.Csv as PC
import qualified Pipes.Prelude as P
import           Pipes.Safe
import qualified System.IO as IO
import           System.Directory
import           System.FilePath
import           System.Log.Logger

import           Vaultaire.Query
import           Vaultaire.Types
import           Chevalier.Client
import           Parse

evalAlign :: FilePath
          -> Source -> Source -> IO ()
evalAlign outfile sauce1 sauce2 = do
  (sd, s1) <- msnd Select <$> retrieve sauce1
  (_,  s2) <- msnd Select <$> retrieve sauce2
  out outfile $ enumerate $ align (sd, s1) s2
  where msnd f (x,y) = (x, f y)

evalExport :: FilePath
           -> URI -> Origin -> TimeStamp -> TimeStamp -> IO ()
evalExport outdir u org start end =
  runSafeT $ runEffect
           $ enumerateOrigin u org >-> fetchAddress
  where fetchAddress = forever $ do
          (addr, sd) <- await
          let addrdir = concat [outdir, "/", show addr, escape $ B8.unpack $ toWire sd]
          let pfile   = concat [addrdir, "/points"]
          h <- liftIO $ do
                createDirectoryIfMissing False addrdir
                B8.appendFile (addrdir ++ "/sd") (toWire sd)
                infoM "Query" $ "Reading points from address " ++ show addr
                IO.openFile pfile IO.WriteMode
          runEffect $   readSimplePoints u addr start end org
                    >-> PC.encode
                    >-> PB.toHandle h
          liftIO $ IO.hClose h
        escape = map (\c -> if isPathSeparator c then '_' else c)

evalFetch :: FilePath
           -> (String, String) -> URI -> Origin -> TimeStamp -> TimeStamp -> IO ()
evalFetch outdir (key,val) u org start end = do
  let pf =  concat [outdir, "/", "points"]
  let df =  concat [outdir, "/", "sd"]
  ph     <- IO.openFile df IO.WriteMode
  addrs  <- getAddresses u org (key,val)
  runSafeT $ runEffect
           $ for (each addrs)
                  (\(a,d) -> do liftIO $ B8.writeFile pf $ toWire d
                                liftIO $ infoM "Query" $ "Reading points from address" ++ show a
                                readSimplePoints u a start end org)
           >-> PC.encode
           >-> PB.toHandle ph

retrieve :: MonadSafe m => Source -> IO (SourceDict, Producer SimplePoint m ())
retrieve (File _ p sd) = do
  dict <- liftIO $ IO.readFile sd
  h    <- liftIO $ IO.openFile p IO.ReadMode
  return ( fromRight mempty $ fromWire $ B8.pack dict
         , PC.decode PC.NoHeader (PB.fromHandle h) >-> hush)
  where hush = P.filter isRight >-> P.map fromRight'

retrieve (Vault u org addr start end) = do
  sd <- fromMaybe mempty <$> getSourceDict u org addr
  return (sd, readSimplePoints u addr start end org)

out :: FilePath -> Producer SimplePoint (SafeT IO) () -> IO ()
out f p = do
  h <- IO.openFile f IO.WriteMode
  IO.hSetBuffering h IO.NoBuffering
  runSafeT $ runEffect $ p >-> PC.encode >-> PB.toHandle h
