{-# LANGUAGE DeriveGeneric, RecordWildCards, TupleSections #-}

import Control.Monad.Except
import Options.Applicative
import Options.Applicative.Types
import qualified Data.ByteString.Char8 as B
import Network.URI
import Text.Read
import GHC.Generics
import Data.Maybe
import Data.Monoid
import Data.Csv hiding (Parser)
import qualified Data.Vector as V
import Data.Either.Combinators
import Pipes
import Pipes.Safe
import qualified Pipes.Prelude as P
import qualified Pipes.Csv     as PC
import qualified Pipes.ByteString as PS
import qualified System.IO as IO
import qualified Data.List as L

import Vaultaire.Query
import Vaultaire.Types
import Marquise.Types


-- the opts parsers in here are buggy, should use "align" from vaultaire.query directly

data Source
   = File  FilePath
   | Vault (URI,Origin,Address,TimeStamp,TimeStamp)
   deriving (Read, Show)

data Mode = Summation Source
          | Align Source Source

data CmdArgs = CmdArgs { output :: FilePath, operation :: Mode }
     deriving Generic

instance FromRecord SimplePoint where
  parseRecord v
    | V.length v == 3 = SimplePoint <$> v .! 0 <*> v.! 1 <*> v.! 2
    | otherwise       = mzero

instance FromField Address where
  parseField = pure . read . B.unpack

instance FromField TimeStamp where
  parseField = pure . read . B.unpack

instance PC.ToRecord SimplePoint where
  toRecord (SimplePoint address timestamp payload) =
    record [ B.pack $ show address
           , B.pack $ show timestamp
           , B.pack $ show payload ]

fuckingReadIt :: Read a => ReadM a
fuckingReadIt = readerAsk >>= return . read

instance Read URI where
  readsPrec _ = map (,"") . maybeToList . parseURI

mode :: Parser Mode
mode = subparser
     $ command "align" (info (helper <*> parse) (progDesc desc))
  where parse = Align <$> option fuckingReadIt (long "source1")
                      <*> option fuckingReadIt (long "source2")
        desc  = "pad out the first series with times from the second"

args :: Parser CmdArgs
args =   CmdArgs
     <$> strOption (  long "output" <> short 'o' <> metavar "OUT"
                   <> help "output file" )
     <*> mode

evalAlign :: Source -> Source -> IO (Producer SimplePoint (SafeT IO) ())
evalAlign sauce1 sauce2
  = return $ enumerate $ align (Select $ retrieve sauce1) (Select $ retrieve sauce2)

retrieve :: MonadSafe m => Source -> Producer SimplePoint m ()
retrieve (File p) = do
  h <- liftIO (IO.openFile p IO.ReadMode)
  PC.decode PC.NoHeader (PS.fromHandle h) >-> hush
  where hush = P.filter isRight >-> P.map fromRight'
retrieve (Vault (uri, org, addr, start, end)) =
  enumerate $ metrics uri org addr start end

out :: FilePath -> Producer SimplePoint (SafeT IO) () -> IO ()
out f p = do
  h <- IO.openFile f IO.WriteMode
  runSafeT $ runEffect $ p >-> PC.encode >-> PS.toHandle h

main :: IO ()
main = do
  CmdArgs{..} <- execParser toplevel
  case operation of
    Align sauce1 sauce2 -> evalAlign sauce1 sauce2 >>= out output

  where toplevel = info (helper <*> args) (fullDesc <> header "Simple query CLI")
