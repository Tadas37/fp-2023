{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use newtype instead of data" #-}
module Main (main) where

import Web.Spock
import Web.Spock.Config
import Control.Monad (forever)
import Data.Text.Encoding (decodeUtf8)
import Shared (webServerPort, SqlRequest(..), SqlErrorResponse(..), SqlSuccessResponse(..))
import Network.HTTP.Types.Status
import DataFrame (DataFrame(..), Column(..))
import InMemoryTables (TableName)
import Data.Time (UTCTime, getCurrentTime)
import Lib4 qualified
import Data.Functor
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Free (Free(..))
import System.Directory (listDirectory, doesFileExist, removeFile)
import System.FilePath (dropExtension, pathSeparator, takeExtension)
import Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVarIO, modifyTVar', readTVar, newTVar)
import Control.Concurrent.STM (readTVar, atomically)
import Control.Concurrent (threadDelay, forkIO)
import Data.Either (partitionEithers)
import Data.List (intercalate, find)

type FileContent = String

type ConcurrentTable = TVar (TableName, DataFrame)
type ConcurrentDb = TVar [ConcurrentTable]
data AppState = AppState { db :: ConcurrentDb }
type ApiAction a = SpockAction () () AppState a

dbDirectory :: String
dbDirectory = "db"

dbFormat :: String
dbFormat = ".yaml"

main :: IO ()
main = do
  dbContents <- liftIO loadDb
  db <- newTVarIO =<< mapM newTVarIO dbContents
  cfg <- defaultSpockCfg () PCNoDatabase (AppState db)
  forkIO $ syncDb db
  runSpock webServerPort (spock cfg app)

app :: SpockM () () AppState ()
app = post root $ do
  appState <- getState
  mreq <- jsonBody' :: ApiAction SqlRequest
  result <- liftIO $ runExecuteIO (db appState) (Lib4.executeSql (query mreq))
  case result of
    Left err -> do
      setStatus badRequest400
      json SqlErrorResponse { errorMessage = err }
    Right df -> json SqlSuccessResponse { dataFrame = df }

getDbTables :: IO [String]
getDbTables = do
  filesInDir <- listDirectory dbDirectory
  return [ dropExtension fileName | fileName <- filesInDir, takeExtension fileName == dbFormat ]

getTableFilePath :: String -> String
getTableFilePath tableName = dbDirectory ++ [pathSeparator] ++ tableName ++ dbFormat


loadDb :: IO [(TableName, DataFrame)]
loadDb = do
  tableNames <- getDbTables
  contents <- mapM (readFile . getTableFilePath) tableNames
  case Lib4.parseTables contents of
    Left err -> do
      putStrLn $ "Error loading tables: " ++ err
      return []
    Right tables -> return tables

syncDb :: ConcurrentDb -> IO ()
syncDb dbVar = forever $ do
  threadDelay $ 1 * 1000 * 1000 -- 1 second
  tables <- atomically $ readTVar dbVar
  mapM_ syncTable tables

syncTable :: ConcurrentTable -> IO ()
syncTable tableVar = do
  (tableName, tableData) <- atomically $ readTVar tableVar
  let serializedTable = Lib4.dataFrameToSerializedTable (tableName, tableData)
  let serializedYAML = Lib4.serializeTableToYAML serializedTable
  writeFile (getTableFilePath tableName) serializedYAML

runExecuteIO :: ConcurrentDb -> Lib4.Execution r -> IO r
runExecuteIO dbRef (Pure r) = return r
runExecuteIO dbRef (Free step) = do
  next <- runStep step
  runExecuteIO dbRef next
  where
    runStep :: Lib4.ExecutionAlgebra a -> IO a
    runStep (Lib4.GetTime next) = getCurrentTime <&> next
    runStep (Lib4.RemoveTable tableName next) = do
        t <- findTable dbRef tableName
        case t of
          Nothing -> return $ next $ Just $ "Table '" ++ tableName ++ "' does not exist."
          Just ref -> do
            atomically $ modifyTVar' dbRef (filter (/= ref))
            removeFile $ getTableFilePath tableName
            return $ next Nothing
    runStep (Lib4.LoadFiles tableNames next) = do
      tablesExist <- filesExist (map getTableFilePath tableNames)
      if tablesExist
        then do
          fileContents <- mapM (readFile . getTableFilePath) tableNames
          return $ next $ Right fileContents
        else
          return $ next $ Left "One or more provided tables does not exist"
    runStep (Lib4.CreateTablee tableName columns next) = do
      let newTable = DataFrame columns [] 
      tableVar <- findOrCreateTable dbRef tableName
      atomically $ modifyTVar' tableVar (\(_, _) -> (tableName, newTable))
      return $ next Nothing
    runStep (Lib4.UpdateTable (tableName, df) next) = do
      tableVar <- findOrCreateTable dbRef tableName
      atomically $ modifyTVar' tableVar (\(_, _) -> (tableName, df))
      return next
findOrCreateTable :: ConcurrentDb -> TableName -> IO ConcurrentTable
findOrCreateTable dbRef tableName = do
  tables <- atomically $ readTVar dbRef
  foundTableVar <- findTableByName tables tableName
  case foundTableVar of
    Just tableVar -> return tableVar
    Nothing -> atomically $ do
      newTableVar <- newTVar (tableName, emptyDataFrame)
      modifyTVar' dbRef (newTableVar :)
      return newTableVar

findTableByName :: [ConcurrentTable] -> TableName -> IO (Maybe ConcurrentTable)
findTableByName tables tableName = do
  withNames <- mapM (\tblVar -> atomically $ do
                                 (name, _) <- readTVar tblVar
                                 return (name, tblVar)) tables
  return $ lookup tableName withNames

findTable :: ConcurrentDb -> TableName -> IO (Maybe ConcurrentTable)
findTable dbRef tableName = do
  tables <- atomically $ readTVar dbRef
  findTableByName tables tableName


emptyDataFrame :: DataFrame
emptyDataFrame = DataFrame [] []  -- idk ar cia teisingai?

filesExist :: [String] -> IO Bool
filesExist files = allM doesFileExist files

allM :: Monad m => (a -> m Bool) -> [a] -> m Bool
allM _ [] = return True
allM p (x:xs) = do
  result <- p x
  if result then allM p xs else return False
