module Control.Concurrent.Persist where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar


import Control.Exception
import Control.Monad.Except
import Control.Monad.Identity

import Control.Monad

import System.AtomicWrite.Writer.String (atomicWithFile)

import qualified Data.ByteString.Lazy as Lazy

import qualified Data.ByteString as B
import Data.ByteString (ByteString)

import Data.Maybe
import Data.SafeCopy
import Data.Serialize (runGetPartial, Result (..), runPut)
import System.IO
import System.Directory

import Test.QuickCheck (Arbitrary(..), Positive(..), quickCheck)


import Pipes.ByteString (hGetSome)
import Pipes (each)
import Pipes.Parse


class (SafeCopy d, SafeCopy (Update d)) => Persistable d where
  type Update d
  update :: Update d -> d -> d

data Action d =  Update (Update d) | Close (STM ()) -- | Flush d


-- | An opaque type wrapping any kind of user data for use in the 'TX' monad.
data Database d = Database
  { state  :: TVar d
  , queue  :: TChan (Action d)
  }

type Err = String

readLog :: Persistable d => FilePath -> d -> IO (Either Err d)
readLog filename initial = doesFileExist filename >>= \case
    False -> return (Right initial)
    True -> decodeDatabase =<< openBinaryFile filename ReadMode


createDatabase :: d -> STM (Database d)
createDatabase d = Database <$> newTVar d <*> newTChan


runWriter :: Persistable d => Database d -> FilePath -> IO ()
runWriter d filename = openBinaryFile filename AppendMode >>= go
  where
    go handle = do
      action <- atomically $ readTChan (queue d)
      case action of
        Update u     -> B.hPut handle (runPut (safePut u)) >> go handle
        Close action -> atomically action

closeDatabase :: Database d -> STM ()
closeDatabase d = do
  v <- newEmptyTMVar
  writeTChan (queue d) (Close $ putTMVar v ())
  takeTMVar v


makeUpdate :: Persistable d => Database d -> Update d -> STM ()
makeUpdate d u = do
  modifyTVar (state d) (update u)
  writeTChan (queue d) (Update u)


liftMaybe :: (MonadError e m) => e -> m (Maybe a) -> m a
liftMaybe e action = action >>= maybe (throwError e) return

draw' :: MonadError Err m => Parser a m a
draw' = liftMaybe "unexpected end of input" draw

decode' :: (MonadError Err m, SafeCopy a) => Parser ByteString m a
decode' = liftMaybe "unexpected end of input" decode

decode :: forall a m. (MonadError Err m, SafeCopy a) => Parser ByteString m (Maybe a)
decode = draw >>= traverse (next . runGetPartial safeGet) where

  next :: Result a -> Parser ByteString m a
  next r = case r of
    Fail    err  _ -> throwError err
    Partial f      -> (f <$> draw') >>= next
    Done a leftovers -> do
      unless (B.null leftovers) $ unDraw leftovers
      return a


decodeDatabase :: Persistable d => Handle -> IO (Either Err d)
decodeDatabase handle = decodeFrom (hGetSome 1024 handle)

decodeFrom :: (Monad m, Persistable d) => Producer ByteString (ExceptT Err m) r -> m (Either Err d)
decodeFrom p = runExceptT (evalStateT (decode' >>= go) p)
  where
    go db = decode >>= \case
      Nothing -> return db
      Just u  -> go (update u db)


splits :: Int -> ByteString -> [ByteString]
splits n str
  | B.length str > n = b : splits n rest
  | otherwise        = [str]

    where (b, rest) = B.splitAt n str

prop_decodes :: (Persistable d, Arbitrary d, Arbitrary (Update d), Eq d)
             => Positive Int -> d -> [Update d] -> Bool
prop_decodes (Positive n) d updates = Right d' == runIdentity (decodeFrom p)
  where
    p = each (splits n encoded)
    encoded = runPut (mconcat [safePut d, safePut updates])
    d' = foldr update d updates
