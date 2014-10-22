{-# LANGUAGE RecordWildCards, TupleSections #-}

import Control.Monad.Trans.Reader
import Options.Applicative
import Options.Applicative.Builder
import Options.Applicative.Types
import Network.URI
import Text.Read
import Data.Maybe

import Vaultaire.Types

type Source   = Either FilePath ReadCmdArgs
type ReadCmdArgs = (URI, Origin, Address, TimeStamp, TimeStamp)

data Mode = Summation Source
          | Align Source Source

data CmdArgs = CmdArgs { output :: FilePath, operation :: Mode }

sauce :: Parser Source
sauce = option readSauce
               (  long "source" <> short 's' <> metavar "SOURCE"
               <> help "where do I get the points?" )
  where readSauce :: ReadM Source
        readSauce = do
          x <- readerAsk
          case (readMaybe x :: Maybe ReadCmdArgs) of
            Just as -> return $ Right as  -- got arguments to read from vaultaire
            Nothing -> return $ Left  x   -- assume it's a file

instance Read URI where
  readsPrec _ = map (,"") . maybeToList . parseURI

mode :: Parser Mode
mode =   (Summation <$> sauce)
     <|> (Align     <$> sauce <*> sauce)

args :: Parser CmdArgs
args =   CmdArgs
     <$> strOption (  long "output" <> short 'o' <> metavar "OUT"
                   <> help "output file" )
     <*> mode

main :: IO ()
main = do
  CmdArgs{..} <- execParser toplevel
  case mode of Align sauce1 sauce2 ->
  where toplevel = info (helper <*> args) (fullDesc <> header "Simple query CLI")
