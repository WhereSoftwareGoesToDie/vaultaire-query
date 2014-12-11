{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverlappingInstances  #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Vaultaire.Query.Base
       (Query)
where

import           Control.Monad.Zip
import           Pipes
import qualified Pipes.Prelude     as P


-- "DSL" -----------------------------------------------------------------------

-- | Query over a producer is just a 'ListT'.
--
--   @Query m r@ over some base monad @m@ and will yield result @r@
--
--   To construct a query:
--
--     * Use 'Select' on a 'Producer', see the 'Pipes' documentation.
--     * Use list comprehensions syntax, example:
--
--         @
--         query :: Query m (Person, Role)
--         query = [ (person, role)
--                 | person <- Select $ people
--                 , age person < 30
--                 , role   <- possibleRoles ]
--
--         people        :: Producer Person m ()
--         possibleRoles :: Query m Role
--         @
--
--       The above query construct a product of @Person@ and @Role@, SQL-style.
--
type Query = ListT

instance Monad m => MonadZip (Query m) where
  mzip (Select p1) (Select p2) = Select $ P.zip p1 p2
