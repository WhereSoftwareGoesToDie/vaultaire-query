{-# LANGUAGE DeriveGeneric, RecordWildCards, TupleSections #-}

import           Data.Monoid
import           GHC.Generics
import           Options.Applicative
import           Options.Applicative.Types
import           Network.URI
import           System.Directory
import           System.Log.Logger
import           Text.Read
import qualified Text.Parsec as P
import qualified Text.Parsec.Error as P

import           Vaultaire.Query
import           Vaultaire.Types
import           Parse
import           Eval

data Mode
  = Align  Source Source
  | Export URI Origin TimeStamp TimeStamp
  | Fetch  (String, String) URI Origin TimeStamp TimeStamp

data CmdArgs
  = CmdArgs { output    :: FilePath
            , retry     :: Policy
            , operation :: Mode }
  deriving Generic

-- wow so lisp
mode :: Parser Mode
mode = subparser
  (  (command "align"
       (info pAlign
             (progDesc $ concat
               ["align discrete two time series, e.g "
               ,"align file:format=csv,points=points.txt,dict=sd.txt "
               ," vault:reader=tcp://foo.com:9999,origin=ABCDEF,address=7yHojf,start=2099-07-07,end=2100-12-22"]
              )
        )
     )
  <> (command "export"
       (info pExport
             (progDesc $ concat
               ["export simple points for all addresses"
               ," e.g. export tcp://foo.com:9999 ABCDEF 0 999"]
             )
        )
     )
  <> (command "fetch"
       (info pFetch
             (progDesc $ concat
               ["fetch data for a metric matching some (key,value), e.g."
               ," fetch hostname=deadbeef tcp://foo.com:9999 ABCDEF 0 999"]
             )
       )
     )
  )
  where pAlign  = Align  <$> sauce  <*> sauce
        pExport = Export <$> uri    <*> origin <*> timestamp <*> timestamp
        pFetch  = Fetch  <$> keyval <*> uri    <*> origin    <*> timestamp <*> timestamp
        sauce = flip argument mempty $
          do s <- readerAsk
             case P.parse pSource "" s of
               Left e  -> readerError $  "Parsing source failed here: "
                                      ++ (showParserErrors $ P.errorMessages e)
               Right x -> return x
        keyval = flip argument mempty $
          do s <- readerAsk
             case P.parse pKeyVal "" s of
               Left e  -> readerError $  "Parsing keyval failed: "
                                      ++ (showParserErrors $ P.errorMessages e)
               Right x -> return x
        uri       = argument (readerAsk >>= maybe (readerError "cannot parse broker URI") return . parseURI) mempty
        origin    = argument (readerAsk >>= maybe (readerError "cannot parse origin") return . readMaybe) mempty
        timestamp = argument (readerAsk >>= maybe (readerError "cannot read timestamp") return . readMaybe) mempty

args :: Parser CmdArgs
args =   CmdArgs
     <$> strOption (  long "output" <> short 'o' <> metavar "OUT"
                   <> help "output file" )
     <*> option    (  readerAsk >>= policies)
                   (  long "retry" <> short 'r' <> metavar "RETRY"
                   <> value ForeverRetry
                   <> help "retry on backend failure? options := forever | no | <number>")
     <*> mode
  where policies s | s == "forever" = return ForeverRetry
                   | s == "no"      = return NoRetry
                   | otherwise      = maybe (readerError "can't read retry policy")
                                            (return . JustRetry)
                                            (readMaybe s)

main :: IO ()
main = do
  updateGlobalLogger "Query" (setLevel INFO)
  infoM "Query" "starting..."
  CmdArgs{..} <- execParser toplevel
  case operation of
    Align sauce1 sauce2 -> evalAlign retry output sauce1 sauce2
    Export u org s e    -> createDirectoryIfMissing True output
                        >> evalExport retry output u org s e
    Fetch q u org s e   -> createDirectoryIfMissing True output
                        >> evalFetch retry output q u org s e

  where toplevel = info (helper <*> args) (fullDesc <> header "Simple query CLI")
