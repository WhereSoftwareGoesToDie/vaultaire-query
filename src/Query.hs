{-# LANGUAGE DeriveGeneric, RecordWildCards, TupleSections #-}

import           Control.Monad.Except
import           Data.Either.Combinators
import           Data.Monoid
import qualified Data.ByteString.Char8 as B8
import           GHC.Generics
import           Options.Applicative
import           Options.Applicative.Types
import           Pipes
import qualified Pipes.ByteString as PS
import qualified Pipes.Csv as PC
import qualified Pipes.Prelude as P
import           Pipes.Safe
import qualified System.IO as IO
import qualified Text.Parsec as P
import qualified Text.Parsec.Error as P

import           Vaultaire.Query
import           Vaultaire.Types
import           Marquise.Types
import           Parse

data Mode = Align Source Source

data CmdArgs = CmdArgs { output :: FilePath, operation :: Mode }
     deriving Generic

mode :: Parser Mode
mode = subparser
     $ command "align" (info (helper <*> parse) (progDesc desc))
  where parse = Align <$> sauce <*> sauce
        desc  = "pad out the first series with times from the second"

sauce :: Parser Source
sauce = argument
  (do s <- readerAsk
      case P.parse pSource "" s of
        Left e  -> readerError $ "Parsing source failed here: " ++ (showParserErrors
                               $ P.errorMessages e)
        Right x -> return x)
  (help $ concat ["source for data points "
                 ,"e.g. \"file:format=csv,path=foo.txt\" or "
                 ,"vault:reader=tcp://foo.com:9999,origin=ABCDEF,address=7yHojf,start=2099-07-07,end=2100-12-22"])

args :: Parser CmdArgs
args =   CmdArgs
     <$> strOption (  long "output" <> short 'o' <> metavar "OUT"
                   <> help "output file" )
     <*> mode

evalAlign :: Source -> Source -> IO (Producer SimplePoint (SafeT IO) ())
evalAlign sauce1 sauce2 = do
  (sd, s1) <- msnd Select <$> retrieve sauce1
  (_,  s2) <- msnd Select <$> retrieve sauce2
  return $ enumerate $ align (sd, s1) s2
  where msnd f (x,y) = (x, f y)

retrieve :: MonadSafe m => Source -> IO (SourceDict, Producer SimplePoint m ())
retrieve (File _ p sd) = do
  dict <- liftIO $ IO.readFile sd
  h    <- liftIO $ IO.openFile p IO.ReadMode
  return $ ( fromRight mempty $ fromWire $ B8.pack dict
           , PC.decode PC.NoHeader (PS.fromHandle h) >-> hush)
  where hush = P.filter isRight >-> P.map fromRight'

-- TODO get the sourcedict for this address, maybe via chevalier
retrieve (Vault uri org addr start end) =
  return $ (mempty, enumerate $ metrics uri org addr start end)

out :: FilePath -> Producer SimplePoint (SafeT IO) () -> IO ()
out f p = do
  h <- IO.openFile f IO.WriteMode
  IO.hSetBuffering h IO.NoBuffering
  runSafeT $ runEffect $ p >-> PC.encode >-> PS.toHandle h

main :: IO ()
main = do
  CmdArgs{..} <- execParser toplevel
  case operation of
    Align sauce1 sauce2 -> evalAlign sauce1 sauce2 >>= out output

  where toplevel = info (helper <*> args) (fullDesc <> header "Simple query CLI")
