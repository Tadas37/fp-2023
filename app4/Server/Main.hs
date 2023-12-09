{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Web.Spock
import Web.Spock.Config
import Data.Text.Encoding (decodeUtf8)
import Shared (webServerPort, SqlRequest (..), SqlErrorResponse (..), SqlSuccessResponse (..))
import Network.HTTP.Types.Status
import DataFrame (DataFrame (..), Column(..))
import InMemoryTables (tableEmployees)
import Data.Time (UTCTime, getCurrentTime)
import Lib3 qualified
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import System.Directory (listDirectory, doesFileExist)
import System.FilePath (dropExtension, pathSeparator, takeExtension)

type ApiAction a = SpockAction () () () a

main :: IO ()
main = do
  cfg <- defaultSpockCfg () PCNoDatabase ()
  runSpock webServerPort (spock cfg app)

app :: SpockM () () () ()
app = do
  post root $ do
    mreq <- jsonBody :: ApiAction (Maybe SqlRequest)
    case mreq of
      Nothing -> do
        setStatus badRequest400
        json SqlErrorResponse { errorMessage = "Request body format is incorrect." }
      Just req -> do
        result <- liftIO $ runExecuteIO $ Lib3.executeSql $ query req
        case result of
          Left err -> do
            setStatus badRequest400
            json SqlErrorResponse { errorMessage = err }
          Right df -> do
            json SqlSuccessResponse { dataFrame = df }

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
    runStep (Lib3.LoadFiles tableNames next) = do
      tablesExist <- filesExist (map getTableFilePath tableNames)
      if tablesExist
        then do
          fileContents <- mapM (readFile . getTableFilePath) tableNames
          return $ next $ Right fileContents
        else
          return $ next $ Left "One or more provided tables does not exist"

    runStep (Lib3.UpdateTable (tableName, df) next) = do
        let serializedTable = Lib3.dataFrameToSerializedTable (tableName, df)
        let yamlContent = Lib3.serializeTableToYAML serializedTable
        writeFile (getTableFilePath tableName) yamlContent
        return next

columnName :: DataFrame.Column -> String
columnName (DataFrame.Column name _) = name 

getTableFilePath :: String -> String
getTableFilePath tableName = "db/" ++ tableName ++ ".yaml"

filesExist :: [String] -> IO Bool
filesExist [tableName] = doesFileExist tableName
filesExist (tableName : xs) = do
  tableExists <- doesFileExist tableName
  rest <- filesExist xs
  return $ tableExists && rest
filesExist _ = return True
  