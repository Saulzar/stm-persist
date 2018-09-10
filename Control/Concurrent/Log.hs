module Control.Concurrent.Log
  ( updateLog
  , closeLog
  , createLog
  , runWriter
  , readLog
  , readLogFile
  , openLog
  , freshLog

  , Log
  , Persistable (..)
  )
where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TMVar


import Control.Exception
import Control.Monad.Except
import Control.Monad.Identity

import Control.Monad
import Data.Traversable

import System.AtomicWrite.Writer.String (atomicWithFile)

import qualified Data.ByteString.Lazy as Lazy

import qualified Data.ByteString as B
import Data.ByteString (ByteString)

import Data.Monoid
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

data Action d =  Update (Update d) | Close (STM ()) | Flush d


-- | An opaque type wrapping any kind of user data for use in the 'TX' monad.
data Log d = Log
  { state  :: TVar d
  , queue  :: TChan (Action d)
  , thread :: Maybe ThreadId
  }

type Err = String

readLogFile :: Persistable d => FilePath -> IO (Either Err d)
readLogFile filename = doesFileExist filename >>= (\case
    False -> return $ Left ("file does not exist: " <> filename)
    True -> decodeLog =<< openBinaryFile filename ReadMode)


safeWrite :: SafeCopy a => Handle -> a -> IO ()
safeWrite handle a = do
  B.hPut handle (runPut (safePut a))
  hFlush handle


createLog :: d -> STM (Log d)
createLog d = Log <$> newTVar d <*> newTChan <*> pure Nothing

readLog :: Log d -> STM d
readLog db = readTVar (state db)

runWriter :: Persistable d => Log d -> FilePath -> IO (Log d)
runWriter db filename
    | isJust (thread db) = error "log already has writer thread"
    | otherwise = do
  d <- atomically $ readTVar (state db)
  t <- forkIO (restart d)
  return db {thread = Just t}

    where
      restart d = do
        atomicWithFile filename $ \handle -> safeWrite handle d
        openBinaryFile filename AppendMode >>= go

      go handle = do
        action <- atomically $ readTChan (queue db)
        case action of
          Update u     -> safeWrite handle u >> go handle
          Flush  d     -> restart d
          Close action -> atomically action

openLog :: Persistable d => FilePath -> IO (Either Err (Log d))
openLog filepath = do
  e <- readLogFile filepath
  for e $ \d -> do
    db <- atomically $ createLog d
    runWriter db filepath

freshLog :: Persistable d => d -> FilePath -> IO (Log d)
freshLog initial filepath = do
  db <- atomically $ createLog initial
  runWriter db filepath


closeLog :: Log d -> STM ()
closeLog d = do
  v <- newEmptyTMVar
  writeTChan (queue d) (Close $ putTMVar v ())
  takeTMVar v


updateLog :: Persistable d => Log d -> Update d -> STM ()
updateLog d u = do
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


decodeLog :: Persistable d => Handle -> IO (Either Err d)
decodeLog handle = decodeFrom (hGetSome 1024 handle)

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
