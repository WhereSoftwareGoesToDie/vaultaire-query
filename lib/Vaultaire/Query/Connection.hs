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
       ( MarquiseReader(..), runMarquiseReader
       , MarquiseContents(..), runMarquiseContents
       , Chevalier(..), runChevalier
       , Postgres(..), runPostgres )

where

import           Control.Monad.Trans.Reader
import qualified Database.PostgreSQL.Simple as PG
import           Network.URI
import           Pipes
import           Pipes.Safe (MonadSafe)
import qualified Pipes.Safe                 as P
import qualified Pipes.Lift                 as P
import qualified System.ZMQ4                as Z

import           Marquise.Client            (SocketState(..))
import           Vaultaire.Query.Base

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
  P.bracket (liftIO $ Z.context)                 (liftIO . Z.term)   $ \ctx  ->
  P.bracket (liftIO $ Z.socket ctx Z.Req)        (liftIO . Z.close)  $ \sock ->
  P.bracket (liftIO $ Z.connect sock $ show uri) (const $ return ()) $ \_    ->
    P.runReaderP (Chevalier sock) p

-- | Runs the Marquise reader daemon in our query environment stack.
runMarquiseReader :: (MonadSafe m)
                  => URI
                  -> Proxy a a' b b' (ReaderT MarquiseReader m) x
                  -> Proxy a a' b b' m x
runMarquiseReader uri = runMarquise uri MarquiseReader

-- | Runs the Marquise contents daemon in our query environment stack.
runMarquiseContents :: (MonadSafe m)
                    => URI
                    -> Proxy a a' b b' (ReaderT MarquiseContents m) x
                    -> Proxy a a' b b' m x
runMarquiseContents uri = runMarquise uri MarquiseContents

runMarquise :: (MonadSafe m)
            => URI
            -> (SocketState -> conn)
            -> Proxy a a' b b' (ReaderT conn m) x
            -> Proxy a a' b b' m x
runMarquise uri mkconn p =
  P.bracket (liftIO $ Z.context)                 (liftIO . Z.term)   $ \ctx  ->
  P.bracket (liftIO $ Z.socket ctx Z.Dealer)     (liftIO . Z.close)  $ \sock ->
  P.bracket (liftIO $ Z.connect sock $ show uri) (const $ return ()) $ \_    ->
    P.runReaderP (mkconn $ SocketState sock $ show uri) p

-- | Runs the Postgres connection in our query environment stack.
runPostgres :: (MonadSafe m)
            => PG.ConnectInfo
            -> Query (ReaderT Postgres m) x
            -> Query m x
runPostgres pginfo (Select act) = Select $
  P.bracket (liftIO $ PG.connect pginfo) (liftIO . PG.close) $ \c ->
            P.runReaderP (Postgres c) act
