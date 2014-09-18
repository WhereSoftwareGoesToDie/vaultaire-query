{-# LANGUAGE
    MultiParamTypeClasses
  , FlexibleContexts
  , FlexibleInstances
  , ConstraintKinds
  , RankNTypes
  , ScopedTypeVariables
  , OverlappingInstances
  , TypeFamilies
  #-}
-- | Provides ways to run a query given a stack of connections.
module Vaultaire.Query.Connection
       ( MarquiseReader, runMarquiseReader, readSimple
       , MarquiseContents, runMarquiseContents, enumerateAddresses
       , Chevalier, runChevalier, chevalier, chevalierTags
       , Postgres(..), runPostgres )

where

import           Control.Monad
import           Control.Monad.Trans.Reader
import           Data.Bifunctor             (bimap)
import           Data.Either
import qualified Data.Text                  as T
import           Data.Text.Encoding         (encodeUtf8)
import qualified Database.PostgreSQL.Simple as PG
import           Network.URI
import           Pipes
import           Pipes.Safe (MonadSafe)
import qualified Pipes.Safe                 as P
import qualified Pipes.Lift                 as P
import qualified System.ZMQ4                as Z

import qualified Chevalier.Util             as C
import qualified Chevalier.Types            as C
import           Marquise.Client            (SocketState(..))
import           Marquise.IO.Util           (consistentEnumerateOrigin, consistentReadSimple)
import           Vaultaire.Query.Base
import           Vaultaire.Types

newtype Chevalier        = Chevalier        (Z.Socket Z.Req)
newtype MarquiseReader   = MarquiseReader   SocketState
newtype MarquiseContents = MarquiseContents SocketState
newtype Postgres         = Postgres PG.Connection

-- | Runs the Chevalier daemon in our query environment stack.
runChevalier :: (MonadSafe m)
             => URI
             -> Query (ReaderT Chevalier m) x
             -> Query m x
runChevalier uri (Select p) = Select $
  P.bracket (liftIO $ Z.context)          (liftIO . Z.term)  $ \ctx  ->
  P.bracket (liftIO $ Z.socket ctx Z.Req) (liftIO . Z.close) $ \sock ->
  P.bracket (liftIO $ Z.connect sock (show uri))
            (const $ liftIO $ Z.disconnect sock (show uri))  $ \_    ->
            P.runReaderP (Chevalier sock) p

-- | Runs the Marquise reader daemon in our query environment stack.
runMarquiseReader :: (MonadSafe m)
                  => URI
                  -> Query (ReaderT MarquiseReader m) x
                  -> Query m x
runMarquiseReader uri = runMarquise uri MarquiseReader

-- | Runs the Marquise contents daemon in our query environment stack.
runMarquiseContents :: (MonadSafe m)
                    => URI
                    -> Query (ReaderT MarquiseContents m) x
                    -> Query m x
runMarquiseContents uri = runMarquise uri MarquiseContents

runMarquise :: (MonadSafe m)
            => URI
            -> (SocketState -> conn)
            -> Query (ReaderT conn m) x
            -> Query m x
runMarquise uri mkconn (Select p) = Select $
  P.bracket (liftIO $ Z.context)             (liftIO . Z.term)  $ \ctx  ->
  P.bracket (liftIO $ Z.socket ctx Z.Dealer) (liftIO . Z.close) $ \sock ->
  P.bracket (liftIO $ Z.connect sock $ show uri)
            (const $ liftIO $ Z.disconnect sock $ show uri)     $ \_    ->
            P.runReaderP (mkconn $ SocketState sock $ broker uri) p
  where broker = maybe "" uriRegName . uriAuthority

-- | Runs the Postgres connection in our query environment stack.
runPostgres :: (MonadSafe m)
            => PG.ConnectInfo
            -> Query (ReaderT Postgres m) x
            -> Query m x
runPostgres pginfo (Select act) = Select $
  P.bracket (liftIO $ PG.connect pginfo) (liftIO . PG.close) $ \c ->
            P.runReaderP (Postgres c) act


-- Wrapped Chevalier Interface -------------------------------------------------

chevalierTags :: Chevalier -> Origin -> [(String, String)] -> Producer (Address, SourceDict) IO ()
chevalierTags c o = chevalier c o . C.buildRequestFromPairs . encodeTags
  where encodeTags = map (join bimap T.pack)

chevalier :: Chevalier -> Origin -> C.SourceRequest -> Producer (Address, SourceDict) IO ()
chevalier (Chevalier sock) origin request = do
  resp <- liftIO sendrecv
  -- this needs some rethinking
  each $ either (error . show) (rights . map C.convertSource) (C.decodeResponse resp)
  where -- hm, query shouldn!l
        sendrecv = do
          Z.send sock [Z.SendMore] $ encodeOrigin origin
          Z.send sock []           $ C.encodeRequest request
          Z.receive sock
        -- too much coercion between chevalier-common and query
        encodeOrigin (Origin x) = encodeUtf8 $ T.pack $ show x

-- Wrapped Marquise Interface --------------------------------------------------

readSimple :: MarquiseReader
           -> Address -> TimeStamp -> TimeStamp -> Origin
           -> Producer SimpleBurst IO ()
readSimple (MarquiseReader c) a s e o = consistentReadSimple a s e o c

enumerateAddresses :: MarquiseContents
                   -> Origin
                   -> Producer (Address, SourceDict) IO ()
enumerateAddresses (MarquiseContents c) o = consistentEnumerateOrigin o c
