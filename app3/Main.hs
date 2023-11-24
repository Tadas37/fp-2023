module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..), liftF)
import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import DataFrame
import Lib3 qualified
import Data.Either (partitionEithers)
import Data.List (intercalate)
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
    runStep (Lib3.LoadFiles tableNames next) = do
        fileContents <- mapM (readFile . getTableFilePath) tableNames
        return $ next fileContents
    runStep (Lib3.ParseTables contents next) = do
        let parsedTables = map Lib3.parseYAMLContent contents
        let (errors, tables) = partitionEithers parsedTables
        if null errors
            then return $ next tables
            else error $ "YAML parsing errors: " ++ intercalate "; " errors
    runStep (Lib3.GetTableDfByName tableName tables next) =
        case lookup tableName tables of
            Just df -> return $ next df
            Nothing -> error $ "Table not found: " ++ tableName
    runStep (Lib3.GetNotSelectTableName statement next) =
        case statement of
            Lib3.DeleteStatement tableName _ -> return $ next tableName
            Lib3.InsertStatement tableName _ _ -> return $ next tableName
            Lib3.UpdateStatement tableName _ _ _ -> return $ next tableName
            _ -> error "No table name for non-select statement"
    
    runStep (Lib3.UpdateTable (tableName, df) next) = do
        let serializedTable = Lib3.dataFrameToSerializedTable (tableName, df)
        let yamlContent = Lib3.serializeTableToYAML serializedTable
        writeFile (getTableFilePath tableName) yamlContent
        return next
    runStep (Lib3.GenerateDataFrame columns rows next) =
        return $ next (DataFrame columns rows)
    runStep (Lib3.GetReturnTableRows parsedStatement tables timeStamp next) = do
      let rows = case parsedStatement of
            Lib3.SelectAll _ _ -> extractAllRows tables
            _ -> error "Unhandled statement type in GetReturnTableRows"
      return $ next rows
      where
        extractAllRows :: [(Lib3.TableName, DataFrame)] -> [Row]
        extractAllRows tbls = concatMap (dfRows . snd) tbls
    
        dfRows :: DataFrame -> [Row]
        dfRows (DataFrame _ rows) = rows
        
    runStep (Lib3.ShowTableFunction (DataFrame.DataFrame columns _) next) = do
        let newDf = DataFrame.DataFrame [DataFrame.Column "ColumnNames" DataFrame.StringType] 
                                        (map (\colName -> [DataFrame.StringValue colName]) (map columnName columns))
        return $ next newDf
    
    columnName :: DataFrame.Column -> String
    columnName (DataFrame.Column name _) = name
    
    formatRow :: [DataFrame.Value] -> String 
    formatRow row = "[" ++ (intercalate "],[" $ map valueToString row) ++ "]"
    
    valueToString :: DataFrame.Value -> String
    valueToString (DataFrame.IntegerValue i) = show i
    valueToString (DataFrame.StringValue s) = "\"" ++ s ++ "\""
    valueToString (DataFrame.BoolValue b) = show b
    valueToString DataFrame.NullValue = "NULL"
    
    getTableFilePath :: String -> String
    getTableFilePath tableName = "db/" ++ tableName ++ ".yaml"
    