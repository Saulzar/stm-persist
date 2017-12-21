module Control.Concurrent.Persist where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Trans.Except

import Control.Monad

import System.AtomicWrite.Writer.String (atomicWithFile)

import qualified Data.ByteString as B
import Data.ByteString (ByteString)

import Data.Maybe
import Data.SafeCopy
import Data.Serialize (runGetPartial, Result (..))
import System.IO
import System.Directory

import Pipes.ByteString (hGetSome)
import Pipes.Parse


class (SafeCopy d, SafeCopy (Update d)) => Persistable d where
  type Update d
  update :: Update d -> d -> d

data Action d = Flush d | Update (Update d) | Close

data Writer d = Writer
  { filename :: FilePath
  , queue  :: TQueue (Action d)
  , thread :: ThreadId
  }

-- | An opaque type wrapping any kind of user data for use in the 'TX' monad.
data Database d = Database
  { state  :: TVar d
  , writer :: Writer d
  }

readDatabase :: FilePath -> d -> IO d
readDatabase filename initial = doesFileExist filename >>= \case
    False -> return initial
    True -> decodeDatabase =<< openBinaryFile logPath ReadMode


-- | Opens the database at the given path or creates a new one.
openDatabase :: Persistable d
             => FilePath  -- ^ Location of the database file.
             -> d  -- ^ Base data. Used when the database file does not exist.
             -> IO (Database d)
openDatabase filename defaultData = undefined
-- closeDatabase :: Database d -> IO ()
-- closeDatabase Database {..} = do
--     atomically $ check =<< isEmptyTQueue logQueue
--     killThread =<< takeMVar serializerTid
--     hClose logHandle


-- replayUpdates :: Persistable d => Database d -> IO ()
-- replayUpdates db = mapDecode (persistently db . replay)
--                              (B.hGetSome (logHandle db) 1024)

liftMaybe :: Monad m => e -> ExceptT e m (Maybe a) -> ExceptT e m a
liftMaybe e action = action >>= maybe (throwE e) return

draw' :: Monad m => Parser a m a
draw' = liftMaybe "unexpected end of input" draw

decode' :: (Monad m, SafeCopy a) => Parser ByteString (ExceptT String m) a
decode' = liftMaybe "unexpected end of input" decode

decode :: (Monad m, SafeCopy a) => Parser ByteString (ExceptT String m) (Maybe a)
decode = draw >>= traverse $ go (runGetPartial safeGet)
    where
      go f = case bytes of
        Fail    err  _ -> throwError err
        Partial f'     -> draw' >>= go f'
        Done a leftovers -> do
          unless (null leftovers) $ undraw leftovers
          return (Just a)


decodeDatabase :: Persistable d => Handle -> IO (Either String d)
decodeDatabase handle = runExceptT $ evalStateT (go decode') (hGetSome 1024 handle)
  where
    go db = decode >>= \case
      Nothing -> db
      Just u  -> go (update u db)


-- mapDecode :: SafeCopy a => (a -> IO ()) -> IO B.ByteString -> IO ()
-- mapDecode f nextChunk = go run =<< nextChunk
--     where
--         run = runGetPartial safeGet
--         go k c = case k c of
--             Fail    err  _ -> error ("TX.mapDecode: " ++ err)
--             Partial k'     -> go k' =<< nextChunk
--             Done    u c'   -> f u >> if B.null c'
--                                        then do c'' <- nextChunk
--                                                if B.null c''
--                                                    then return ()
--                                                    else go run c''
--                                        else go run c'
------------------------------------------------------------------------------

-- writer :: Persistable d => Database d -> IO ()
-- writer Database {..} = forever $ do
--     u <- atomically $ readTQueue logQueue
--     let str = runPut (safePut u)
--     B.hPut logHandle str
