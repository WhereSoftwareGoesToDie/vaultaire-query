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
       , Postgres(..), runPostgres
       , MonadSafeIO, safeLiftIO, runSafeIO
       , In(..))
where

import           Control.Error.Util (syncIO)
import           Control.Exception
import           Control.Monad.Error
import           Control.Monad.Trans.Either (runEitherT)
import           Control.Monad.Trans.Reader
import           Data.Bifunctor (bimap)
import           Data.Either
import qualified Data.Text                  as T
import           Data.Text.Encoding (encodeUtf8)
import qualified Database.PostgreSQL.Simple as PG
import           Pipes
import qualified Pipes.Lift                 as P
import qualified System.ZMQ4                as Z

import qualified Chevalier.Util             as C
import qualified Chevalier.Types            as C
import           Marquise.Client  (SocketState(..))
import           Marquise.IO.Util (consistentEnumerateOrigin, consistentReadSimple)
import           Vaultaire.Query.Base
import           Vaultaire.Types

newtype Chevalier        = Chevalier        SocketState
newtype MarquiseReader   = MarquiseReader   SocketState
newtype MarquiseContents = MarquiseContents SocketState
newtype Postgres         = Postgres PG.Connection


-- | Runs the Chevalier daemon in our query environment stack.
runChevalier :: (MonadSafeIO m)
             => String
             -> Query (ReaderT Chevalier m) x
             -> Query m x
runChevalier url = runZMQ url Chevalier

-- | Runs the Marquise reader daemon in our query environment stack.
runMarquiseReader :: (MonadSafeIO m)
                  => String
                  -> Query (ReaderT MarquiseReader m) x
                  -> Query m x
runMarquiseReader broker = runZMQ ("tcp://" ++ broker ++ ":5570") MarquiseReader

-- | Runs the Marquise contents daemon in our query environment stack.
runMarquiseContents :: (MonadIO m, MonadError SomeException m)
                    => String
                    -> Query (ReaderT MarquiseContents m) x
                    -> Query m x
runMarquiseContents broker = runZMQ ("tcp://" ++ broker ++ ":5580") MarquiseContents

runZMQ :: (MonadSafeIO m)
            => String
            -> (SocketState -> conn)
            -> Query (ReaderT conn m) x
            -> Query m x
runZMQ str mkconn (Select p) = Select $
  bracketSafe (safeLiftIO $ Z.context) (safeLiftIO . Z.term) $ \ctx ->
    bracketSafe (safeLiftIO $ Z.socket ctx Z.Dealer) (safeLiftIO . Z.close) $ \sock ->
      bracketSafe (safeLiftIO $ Z.connect sock str) (const $ safeLiftIO $ Z.disconnect sock str) $ \_ ->
        P.runReaderP (mkconn $ SocketState sock str) p

-- | Runs the Postgres connection in our query environment stack.
runPostgres :: (MonadSafeIO m)
            => PG.ConnectInfo
            -> Query (ReaderT Postgres m) x
            -> Query m x
runPostgres pginfo (Select act) = Select $
  bracketSafe (safeLiftIO $ PG.connect pginfo) (safeLiftIO . PG.close) $ \c ->
    P.runReaderP (Postgres c) act


-- Wrapped Chevalier Interface -------------------------------------------------

chevalierTags :: Chevalier -> Origin -> [(String, String)] -> Producer (Address, SourceDict) IO ()
chevalierTags c o = chevalier c o . C.buildRequestFromPairs . encodeTags
  where encodeTags = map (join bimap T.pack)

chevalier :: Chevalier -> Origin -> C.SourceRequest -> Producer (Address, SourceDict) IO ()
chevalier (Chevalier (SocketState sock _)) origin request = do
  resp <- liftIO sendrecv
  -- this needs some rethinking
  each $ either (error . show) (rights . map C.convertSource) (C.decodeResponse resp)
  where -- hm, query shouldn't have to do this
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


-- Running a Query -------------------------------------------------------------

-- | Constraint a transformer stack @haystack@ to have a transformer @needle@ anywhere in the stack.
--   This relies on @OverlappingInstances@ to find the position of @needle@ in @haystack@,
--   we can use closed type families instead if needed.
class Monad haystack => In needle haystack where
  liftT :: (forall m. Monad m => needle m a) -> haystack a

instance (Monad m, Monad (t m)) => In t (t m) where
  liftT t = t

instance (In t s, MonadTrans t', Monad s, Monad (t' s)) => In t (t' s) where
  liftT t = lift (liftT t)

-- | IO-capable monad that can be used with @safeLiftIO@ to bracket exceptions.
type MonadSafeIO m = (MonadIO m, MonadError SomeException m)

-- | Like @LiftIO@, but ensure that resources are cleaned up even if an IO exception occurs.
safeLiftIO :: MonadSafeIO m => IO a -> m a
safeLiftIO = either (throwError) (return) <=< runEitherT . syncIO

-- | Like @bracket@, but convert IO exceptions into errors so clean-up can be run.
bracketSafe :: MonadSafeIO m => m a -> (a -> m b) -> (a -> m c) -> m c
bracketSafe start finish act = do
  a <- start
  catchError (do ret <- act a
                 _   <- finish a
                 return ret)
             (\(exc :: SomeException) -> finish a >> throwError exc)

-- orphaned instance of Error since we want to rethrow bracketed IO exceptions
-- should never be run, this is just to satify the Error constraint
-- should be removed when pipes is updated to use the new Except
instance Error SomeException where
  strMsg = error

-- | Runs the last layer of the transformer stack, which encapsulates exceptions as errors.
--   This rethrows those exceptions into IO (this is needed to clean up resources).
runSafeIO :: MonadIO m
          => Query (ErrorT SomeException m) x
          -> Query m x
runSafeIO (Select p) = Select $
  P.runErrorP p >>= either throw return
