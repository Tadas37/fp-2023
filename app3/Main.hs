module Main (main) where

import System.Directory (doesFileExist)
import Data.List (find, isPrefixOf)
import Data.Maybe (mapMaybe,fromMaybe)
import Data.List (findIndex)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..), liftF)
import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import DataFrame
    ( Column(..),
      ColumnType(StringType),
      DataFrame(..),
      Row,
      Value(StringValue, IntegerValue, BoolValue) )
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


    runStep (Lib3.IsParsedStatementValid parsedStatement tables next) = do
      let isValid = Lib3.validateStatement parsedStatement tables
      return $ next isValid


    runStep (Lib3.UpdateRows parsedStatement tables next) = do
      let updatedTable = updateTable parsedStatement tables
      case updatedTable of
          Just tbl -> return $ next tbl
          Nothing -> error "Table not found for updating rows"
      where
        updateTable :: Lib3.ParsedStatement -> [(Lib3.TableName, DataFrame)] -> Maybe (Lib3.TableName, DataFrame)
        updateTable stmt tbls =
            case stmt of
                Lib3.UpdateStatement tableName _ _ _ -> do
                    df <- lookup tableName tbls
                    return (tableName, Lib3.updateRowsInTable stmt df)
                _ -> Nothing
          

    runStep (Lib3.ParseSql statement next) = 
      case Lib3.parseStatement statement of
        Right parsedStatement -> return $ next parsedStatement
        Left error -> return $ next $ Lib3.Invalid error

    runStep (Lib3.DeleteRows parsedStatement tables next) = 
      case parsedStatement of
          Lib3.DeleteStatement tableName whereClause ->
              case lookup tableName tables of
                  Just df -> do
                      let updatedDf = Lib3.filterRows df whereClause
                      case updatedDf of
                          Right dfFiltered -> 
                              runStep (Lib3.UpdateTable (tableName, dfFiltered) (next (tableName, dfFiltered)))
                          Left errMsg -> error errMsg
                  Nothing -> error $ "Table not found: " ++ tableName
          Lib3.SelectAll _ _ -> error "SelectAll not valid for DeleteRows"
          Lib3.SelectAggregate _ _ _ -> error "SelectAggregate not valid for DeleteRows"
          Lib3.SelectColumns _ _ _ -> error "SelectColumns not valid for DeleteRows"
          Lib3.InsertStatement _ _ _ -> error "InsertStatement not valid for DeleteRows"
          Lib3.UpdateStatement _ _ _ _ -> error "UpdateStatement not valid for DeleteRows"
          Lib3.ShowTableStatement _ -> error "ShowTableStatement not valid for DeleteRows"
          Lib3.ShowTablesStatement -> error "ShowTablesStatement not valid for DeleteRows"
          Lib3.Invalid _ -> error "Invalid statement cannot be processed in DeleteRows"
  
    runStep (Lib3.InsertRows parsedStatement tables next) =
        case parsedStatement of
            Lib3.InsertStatement tableName maybeSelectedColumns row ->
                case lookup tableName tables of
                    Just (DataFrame cols tableRows) -> do
                        let columnNames = fmap Lib3.extractColumnNames maybeSelectedColumns
                        let newRow = Right row 
                        case newRow of
                            Right row -> do
                                let updatedDf = DataFrame cols (tableRows ++ [row])
                                runStep (Lib3.UpdateTable (tableName, updatedDf) (next (tableName, updatedDf)))
                            Left errMsg -> error errMsg
                    Nothing -> error $ "Table not found: " ++ tableName
            Lib3.SelectAll _ _ -> error "SelectAll not valid for InsertRows"
            Lib3.SelectAggregate _ _ _ -> error "SelectAggregate not valid for InsertRows"
            Lib3.SelectColumns _ _ _ -> error "SelectColumns not valid for InsertRows"
            Lib3.DeleteStatement _ _ -> error "DeleteStatement not valid for InsertRows"
            Lib3.UpdateStatement _ _ _ _ -> error "UpdateStatement not valid for InsertRows"
            Lib3.ShowTableStatement _ -> error "ShowTableStatement not valid for InsertRows"
            Lib3.ShowTablesStatement -> error "ShowTablesStatement not valid for InsertRows"
            Lib3.Invalid _ -> error "Invalid statement cannot be processed in InsertRows"


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
