--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

import Data.List
import Data.Function
import Pipes
import qualified Pipes.Prelude as P
import qualified Data.Map      as M
import Test.Hspec
import Test.QuickCheck

import Vaultaire.Query

main :: IO ()
main =
    hspec $ do
        describe "combinator: cacheQ" $ do
            it "caches the results of a (key,val) query" $ property queryCache

queryCache :: [Int] -> [Char] -> Bool
queryCache keys values =
  let query  = foreach $ zip (nub keys) values
      cached = head $ P.toList $ enumerate $ cacheQ query
      actual =        P.toList $ enumerate          query
  in  M.toList cached == (sortBy (compare `on` fst) actual)
