import Rates

import           Data.Maybe
import           Data.Csv
import           Network.URI
import           Pipes
import           Pipes.Safe
import qualified Pipes.Prelude    as P

import           Vaultaire.Query

main :: IO ()
main = do queryRespRates <- run $ respRates origin start end
          queryCpuRates  <- run $ cpuRates origin start end
          putStrLn $ show $ encode $ queryCpuRates
  where origin   = read "ABCDEF"
        start    = read "2014-08-03"
        end      = read "2014-08-04"
        mr       = fromJust $ parseURI "tcp://chateau-02.syd1.anchor.net.au:5570"
        mc       = fromJust $ parseURI "tcp://chateau-02.syd1.anchor.net.au:5580"
        run      = runSafeT . P.toListM . every . runMarquiseReader mr . runMarquiseContents mc
