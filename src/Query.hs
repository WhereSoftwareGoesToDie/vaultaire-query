{-# LANGUAGE DeriveGeneric, RecordWildCards, TupleSections #-}

import           Control.Monad.Except
import           Data.Either.Combinators
import           Data.Monoid
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
evalAlign sauce1 sauce2
  = return $ enumerate $ align (Select $ retrieve sauce1) (Select $ retrieve sauce2)

retrieve :: MonadSafe m => Source -> Producer SimplePoint m ()
retrieve (File _ p) = do
  h <- liftIO (IO.openFile p IO.ReadMode)
  PC.decode PC.NoHeader (PS.fromHandle h) >-> hush
  where hush = P.filter isRight >-> P.map fromRight'

retrieve (Vault uri org addr start end) =
  enumerate $ metrics uri org addr start end

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
