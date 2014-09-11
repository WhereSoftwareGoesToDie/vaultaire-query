import Rates

import           Control.Monad.Trans.Reader
import qualified Data.ByteString.Char8 as B
import           Data.Word
import           Data.Csv
import qualified Data.Vector      as V
import           Data.Binary.IEEE754
import           Network.URI
import           Pipes
import qualified Pipes.Prelude    as P
import qualified Pipes.Csv        as P

import           Vaultaire.Types
import           Marquise.Types
import           Vaultaire.Query
import           Vaultaire.Control.Lift
import           Vaultaire.Control.Safe

main :: IO ()
main = do queryRespRates <- run $ respRates origin start end
          queryCpuRates  <- run $ cpuRates origin start end
          putStrLn $ show $ encode $ queryCpuRates
  where origin = read "R82KX1"
        start = read "2014-08-03"
        end   = read "2014-08-04"
        mkURI :: Int -> URI
        mkURI port = nullURI { uriScheme = "tcp:"
                             , uriAuthority = Just $ URIAuth { uriRegName  = "chateau-02.syd1.anchor.net.au"
                                                             , uriPort     = ":" ++ show port
                                                             , uriUserInfo = "" } }
        run = P.toListM . every . runSafeIO . runMarquiseReader (mkURI 5570) . runMarquiseContents (mkURI 5580)
