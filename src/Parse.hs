{-# LANGUAGE OverloadedStrings #-}

module Parse
      ( Source(..), Format(..)
      , pSource, showParserErrors)
where

import           Control.Applicative hiding ((<|>))
import           Control.Monad.Except
import qualified Data.ByteString.Char8 as B
import           Data.Char
import           Data.Binary.IEEE754
import           Data.Csv hiding (Parser)
import qualified Data.Csv as C
import qualified Data.Vector as V
import           Data.Word
import           Network.URI
import           Text.Parsec
import           Text.Parsec.String
import           Text.Parsec.Error
import           Text.Read

import           Vaultaire.Types
import           Marquise.Types

data Source
   = File  Format FilePath FilePath
   | Vault URI Origin Address TimeStamp TimeStamp

data Format = CSV
     deriving Read

pTillSep :: Parser String
pTillSep = manyTill anyChar (try (string ","))

pTillEOF :: Parser String
pTillEOF = manyTill anyChar (try eof)

pRead :: Read a => String -> String -> Parser a
pRead msg thing = maybe (unexpected msg) return (readMaybe thing)

-- | Source:
--   "file:format=csv,path=foo.txt"
--   "vault:reader=tcp://foo.com:9999,origin=ABCDEF,address=7yHojf,start=2099-07-07,end=2100-12-22"
--
pSource :: Parser Source
pSource = pFile <|> pVault
  where pFile, pVault :: Parser Source
        pFile = do
          _  <- try $ string "file:"
          f  <- string "format="  >> pTillSep >>= pRead "unrecognised format" . map toUpper
          p  <- string "points="  >> pTillSep
          d  <- string "dict="    >> pTillEOF
          return $ File f p d
        pVault = do
          _  <- try $ string "vault:"
          x  <- string "reader="  >> pTillSep
          u  <- maybe (unexpected "can't parse reader URI") return (parseURI x)
          o  <- string "origin="  >> pTillSep >>= pRead "can't parse origin"
          a  <- string "address=" >> pTillSep >>= pRead "can't parse address"
          s  <- string "start="   >> pTillSep >>= pRead "can't parse timestamp"
          e  <- string "end="     >> pTillEOF >>= pRead "can't parse timestamp"
          return $ Vault u o a s e

showParserErrors :: [Message] -> String
showParserErrors = showErrorMessages "" "Unknown" "Expecting" "Unexpected" "EOF"

instance FromRecord SimplePoint where
  parseRecord v
    | V.length v == 3 = SimplePoint <$> v .! 0 <*> v .! 1 <*> parseVal (v V.! 2)
    | otherwise       = mzero

parseVal :: Field -> C.Parser Word64
parseVal = maybe mzero pure . fmap doubleToWord . readMaybe . filter (\c -> isNumber c || c == '.') . B.unpack

instance FromField Address where
  parseField = maybe mzero pure . readMaybe . B.unpack

instance FromField TimeStamp where
  parseField = maybe mzero pure . readMaybe . B.unpack

instance ToRecord SimplePoint where
  toRecord (SimplePoint address timestamp payload) =
    record [ B.pack $ show address
           , B.pack $ show timestamp
           , B.pack $ show $ wordToDouble payload ]
