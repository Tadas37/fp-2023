{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE RankNTypes #-}

module Lib3
  ( executeSql,
    parseTables,
    Execution,
    ExecutionAlgebra(..),
    ParsedStatement(..),
    SelectColumn(..),
    TableName,
    loadFiles,
    getTime,
    parseYAMLContent,
    getTableDfByName,
    dataFrameToSerializedTable,
    serializeTableToYAML,
    validateStatement,
    SerializedTable(..),
    updateRowsInTable,
    parseStatement,
    filterRows,
    filterDataFrame,
    rowSatisfiesWhereClause,
    conditionSatisfied,
    compareWithCondition,
    compareValue,
    findColumnIndex,
    extractSelectedColumnsRows,
    extractAggregateRows,
    extractColumnNames,
    WhereClause(..),
    Condition(..),
    ConditionValue(..),
  )
where

import Control.Monad.Free (Free (..), liftF)

import DataFrame (Column(..), ColumnType(..), Value(..), Row, DataFrame(..))
import Data.Yaml (decodeEither')
import Data.Text as T ( unpack, pack )
import Data.List
    ( (++),
      zip,
      map,
      elemIndex,
      elem,
      length,
      null,
      all,
      words,
      break,
      drop,
      dropWhile,
      head,
      init,
      last,
      intercalate,
      isPrefixOf,
      isSuffixOf,
      partition,
      concatMap,
      find,
      lookup,
      any,
      notElem,
      filter,
      findIndex,
      (!!),
      isInfixOf, insert )
import qualified Data.Yaml as Y
import Data.Char (toLower, isDigit)
import Data.Time (UTCTime)
import qualified Data.Aeson.Key as Key
import qualified Data.ByteString.Char8 as BS
import Data.Either (isRight, isLeft, partitionEithers)
import Prelude hiding (zip)
import Data.Maybe ( mapMaybe, isJust, isNothing, fromMaybe )
import Control.Monad (zipWithM)

type TableName = String
type FileContent = String
type ErrorMessage = String
type SQLQuery = String
type ColumnName = String

data SerializedTable = SerializedTable
  { tableName :: TableName
  , columns   :: [SerializedColumn]
  , rows      :: [[Y.Value]]
  }

data SerializedColumn = SerializedColumn
  { name     :: String
  , dataType :: String
  }

instance Y.FromJSON SerializedTable where
  parseJSON = Y.withObject "SerializedTable" $ \v -> SerializedTable
    <$> v Y..: Key.fromString "tableName"
    <*> v Y..: Key.fromString "columns"
    <*> v Y..: Key.fromString "rows"

instance Y.FromJSON SerializedColumn where
  parseJSON = Y.withObject "SerializedColumn" $ \v -> SerializedColumn
    <$> v Y..: Key.fromString "name"
    <*> v Y..: Key.fromString "dataType"

data SelectColumn
  = Now
  | TableColumn TableName ColumnName
  | Max TableName ColumnName
  | Avg TableName ColumnName
  deriving (Show, Eq)

type SelectedColumns = [SelectColumn]
type SelectedTables = [TableName]

data ParsedStatement
  = SelectAll SelectedTables (Maybe WhereClause)
  | SelectAggregate SelectedTables SelectedColumns (Maybe WhereClause)
  | SelectColumns SelectedTables SelectedColumns (Maybe WhereClause)
  | DeleteStatement TableName (Maybe WhereClause)
  | InsertStatement TableName (Maybe SelectedColumns) Row
  | UpdateStatement TableName SelectedColumns Row (Maybe WhereClause)
  | ShowTableStatement TableName
  | ShowTablesStatement
  | Invalid ErrorMessage
  deriving (Show, Eq)

data WhereClause
  = IsValueBool Bool TableName String
  | Conditions [Condition]
  deriving (Show, Eq)

data Condition
  = Equals SelectColumn ConditionValue
  | GreaterThan SelectColumn ConditionValue
  | LessThan SelectColumn ConditionValue
  | LessthanOrEqual SelectColumn ConditionValue
  | GreaterThanOrEqual SelectColumn ConditionValue
  | NotEqual SelectColumn ConditionValue
  deriving (Show, Eq)

data ConditionValue
  = StrValue String
  | IntValue Integer
  deriving (Show, Eq)

data SelectType = Aggregate | ColumnsAndTime | AllColumns

data StatementType = Select | Delete | Insert | Update | ShowTable | ShowTables | InvalidStatement

data ExecutionAlgebra next
  = LoadFiles [TableName] (Either ErrorMessage [FileContent] -> next)
  | UpdateTable (TableName, DataFrame) next
  | GetTime (UTCTime -> next)
  | ParseSql SQLQuery (ParsedStatement -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFiles :: [TableName] -> Execution (Either ErrorMessage [FileContent])
loadFiles names = liftF $ LoadFiles names id

parseTables :: [FileContent] -> Either String [(TableName, DataFrame)]
parseTables contents = 
    let parsedTables = map parseYAMLContent contents
        (errors, tables) = partitionEithers parsedTables
    in if null errors
        then Right tables
        else Left $ "YAML parsing errors: " ++ intercalate "; " errors


getTableDfByName :: TableName -> [(TableName, DataFrame)] -> Either ErrorMessage DataFrame
getTableDfByName tableName tables =
    case lookup tableName tables of
        Just df -> Right df
        Nothing -> Left $ "Table not found: " ++ tableName


updateTable :: (TableName, DataFrame) -> Execution ()
updateTable table = liftF $ UpdateTable table ()

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

parseSql :: SQLQuery -> Execution ParsedStatement
parseSql query = liftF $ ParseSql query id

getTableNames :: Lib3.ParsedStatement -> [Lib3.TableName]
getTableNames (Lib3.SelectAll tableNames _) = tableNames
getTableNames (Lib3.SelectAggregate tableNames _ _) = tableNames
getTableNames (Lib3.SelectColumns tableNames _ _) = tableNames
getTableNames (Lib3.DeleteStatement tableName _) = [tableName]
getTableNames (Lib3.InsertStatement tableName _ _) = [tableName]
getTableNames (Lib3.UpdateStatement tableName _ _ _) = [tableName]
getTableNames (Lib3.ShowTableStatement tableName) = [tableName]
getTableNames Lib3.ShowTablesStatement = ["employees", "employees1", "animals"]
getTableNames (Lib3.Invalid _) = []
getTableNames _ = [] 


deleteRows :: ParsedStatement -> [(TableName, DataFrame)] -> (TableName, DataFrame)
deleteRows (DeleteStatement tableName maybeWhereClause) tables =
    case lookup tableName tables of
        Just df ->
            case maybeWhereClause of
                Just whereClause ->
                    case Lib3.filterRows df maybeWhereClause of
                        Right dfFiltered -> (tableName, dfFiltered)
                        Left errMsg -> error errMsg
                Nothing -> (tableName, df) 
        Nothing -> error $ "Table not found: " ++ tableName
deleteRows (SelectAll _ _) _ = error "SelectAll not valid for DeleteRows"
deleteRows (SelectAggregate _ _ _) _ = error "SelectAggregate not valid for DeleteRows"
deleteRows (SelectColumns _ _ _) _ = error "SelectColumns not valid for DeleteRows"
deleteRows (InsertStatement _ _ _) _ = error "InsertStatement not valid for DeleteRows"
deleteRows (UpdateStatement _ _ _ _) _ = error "UpdateStatement not valid for DeleteRows"
deleteRows (ShowTableStatement _) _ = error "ShowTableStatement not valid for DeleteRows"
deleteRows ShowTablesStatement _ = error "ShowTablesStatement not valid for DeleteRows"
deleteRows (Invalid _) _ = error "Invalid statement cannot be processed in DeleteRows"

insertRows :: Lib3.ParsedStatement -> [(Lib3.TableName, DataFrame)] -> (Lib3.TableName, DataFrame)
insertRows (Lib3.InsertStatement tableName maybeSelectedColumns row) tables =
    case lookup tableName tables of
        Just (DataFrame cols tableRows) ->
            let
                columnNames = fmap Lib3.extractColumnNames maybeSelectedColumns
                newRow = Right row
            in
                case newRow of
                    Right newRowData ->
                        let updatedDf = DataFrame cols (tableRows ++ [newRowData])
                        in (tableName, updatedDf)
                    Left errMsg -> error errMsg
        Nothing -> error $ "Table not found: " ++ tableName
insertRows (Lib3.SelectAll _ _) _ = error "SelectAll not valid for InsertRows"
insertRows (Lib3.SelectAggregate _ _ _) _ = error "SelectAggregate not valid for InsertRows"
insertRows (Lib3.SelectColumns _ _ _) _ = error "SelectColumns not valid for InsertRows"
insertRows (Lib3.DeleteStatement _ _) _ = error "DeleteStatement not valid for InsertRows"
insertRows (Lib3.UpdateStatement _ _ _ _) _ = error "UpdateStatement not valid for InsertRows"
insertRows (Lib3.ShowTableStatement _) _ = error "ShowTableStatement not valid for InsertRows"
insertRows Lib3.ShowTablesStatement _ = error "ShowTablesStatement not valid for InsertRows"
insertRows (Lib3.Invalid _) _ = error "Invalid statement cannot be processed in InsertRows"

updateRows :: ParsedStatement -> [(TableName, DataFrame)] -> Maybe (TableName, DataFrame)
updateRows (UpdateStatement tableName selectedColumns newRow maybeWhereClause) tables =
    case lookup tableName tables of
        Just df -> Just (tableName, updateRowsInTable df newRow maybeWhereClause)
        Nothing -> Nothing
    where
        updateRowsInTable :: DataFrame -> Row -> Maybe WhereClause -> DataFrame
        updateRowsInTable (DataFrame columns rows) newRow whereClause =
            let updatedRows = map (updateRowIfMatches newRow whereClause) rows
            in DataFrame columns updatedRows

        updateRowIfMatches :: Row -> Maybe WhereClause -> Row -> Row
        updateRowIfMatches newRow Nothing _ = newRow
        updateRowIfMatches newRow (Just whereClause) row =
            if rowMatchesWhereClause row whereClause
            then newRow
            else row

        rowMatchesWhereClause :: Row -> WhereClause -> Bool
        -- Implementation for matching a row with a where clause
        rowMatchesWhereClause _ _ = False -- Placeholder, implement based on your WhereClause definition

updateRows _ _ = Nothing


getReturnTableRows :: ParsedStatement -> [(TableName, DataFrame)] -> UTCTime -> [Row]
getReturnTableRows parsedStatement tables timeStamp = 
    case parsedStatement of
        SelectAll _ _ -> extractAllRows tables
        SelectColumns tableNames conditions _ -> Lib3.extractSelectedColumnsRows tableNames conditions tables
        SelectAggregate tableNames aggFunc conditions -> Lib3.extractAggregateRows tableNames aggFunc conditions tables
        _ -> error "Unhandled statement type in getReturnTableRows"
    where
        extractAllRows :: [(TableName, DataFrame)] -> [Row]
        extractAllRows tbls = concatMap (dfRows . snd) tbls

        dfRows :: DataFrame -> [Row]
        dfRows (DataFrame _ rows) = rows


generateDataFrame :: [Column] -> [Row] -> DataFrame
generateDataFrame columns rows = DataFrame columns rows

showTablesFunction :: [TableName] -> DataFrame
showTablesFunction tables = 
    let column = Column "tableName" StringType
        rows = map (\name -> [StringValue name]) tables
    in DataFrame [column] rows


showTableFunction :: DataFrame -> DataFrame
showTableFunction (DataFrame columns _) =
    let newDf = DataFrame [Column "ColumnNames" StringType] 
                          (map (\(Column colName _) -> [StringValue colName]) columns)
    in newDf


executeSql :: SQLQuery -> Execution (Either ErrorMessage DataFrame)
executeSql statement = do
  parsedStatement <- parseSql statement
  let tableNames = getTableNames parsedStatement
  tableFiles <- loadFiles tableNames
  case tableFiles of
    Left err -> return $ Left err
    Right content -> 
      case parseTables content of
        Left parseErr -> return $ Left parseErr
        Right tables -> do
          let statementType = getStatementType statement
          timeStamp     <- getTime
          let (isValid, errorMessage) = validateStatement parsedStatement tables
          if not isValid
            then return $ Left errorMessage
            else
            case statementType of
              Select -> do
                let columns = getSelectedColumns parsedStatement tables
                let rows = getReturnTableRows parsedStatement tables timeStamp
                let df = generateDataFrame columns rows
                return $ Right df
              Delete -> do
                let (name, df) = deleteRows parsedStatement tables
                updateTable (name, df)
                return $ Right df
              Insert -> do
                let (name, df) = insertRows parsedStatement tables
                updateTable (name, df)
                return $ Right df
              Update -> do
                let updatedTable = updateRows parsedStatement tables
                case updatedTable of
                    Just (name, df) -> do
                        updateTable (name, df)
                        return $ Right df
                    Nothing -> return $ Left "Table not found for updating rows"
              ShowTables -> do
                let allTables = showTablesFunction tableNames
                return $ Right allTables
              ShowTable -> executeShowTable parsedStatement tables
              InvalidStatement -> return $ Left "Invalid statement"
        
executeShowTable :: ParsedStatement -> [(TableName, DataFrame)] -> Execution (Either ErrorMessage DataFrame)
executeShowTable parsedStatement tables = do
  let nonSelectTableName = getNonSelectTableNameFromStatement parsedStatement
  case getTableDfByName nonSelectTableName tables of
    Left errMsg -> return $ Left errMsg
    Right df -> do
      let allTables = showTableFunction df
      return $ Right allTables

getNonSelectTableNameFromStatement :: ParsedStatement -> TableName
getNonSelectTableNameFromStatement (ShowTableStatement tableName) = tableName
getNonSelectTableNameFromStatement _ = error "Non-select statement expected"


updateRowsInTable :: ParsedStatement -> DataFrame -> DataFrame
updateRowsInTable (UpdateStatement _ _ newRow maybeWhereClause) (DataFrame columns rows) =
   DataFrame columns (Data.List.map (updateRowIfMatches newRow maybeWhereClause) rows)
updateRowsInTable _ df = df

updateRowIfMatches :: Row -> Maybe WhereClause -> Row -> Row
updateRowIfMatches newRow (Just whereClause) row =
    if rowMatchesWhereClause row whereClause
    then newRow
    else row
updateRowIfMatches newRow Nothing _ = newRow

rowMatchesWhereClause :: Row -> WhereClause -> Bool
rowMatchesWhereClause _ (IsValueBool {}) = False
rowMatchesWhereClause row (Conditions conditions) = Data.List.all (rowMatchesCondition row) conditions

rowMatchesCondition :: Row -> Condition -> Bool
rowMatchesCondition _ (Equals (TableColumn _ _) (IntValue _)) = False

parseYAMLContent :: FileContent -> Either ErrorMessage (TableName, DataFrame)
parseYAMLContent content =
  case decodeEither' (BS.pack content) of
    Left err -> Left $ "YAML parsing error: " Data.List.++ show err
    Right serializedTable -> Right $ convertToDataFrame serializedTable

convertToDataFrame :: SerializedTable -> (TableName, DataFrame)
convertToDataFrame st = (tableName st, DataFrame (Data.List.map convertColumn $ columns st) (convertRows $ rows st))

convertColumn :: SerializedColumn -> Column
convertColumn sc = Column (name sc) (convertColumnType $ dataType sc)

convertColumnType :: String -> ColumnType
convertColumnType dt = case dt of
    "integer" -> IntegerType
    "string" -> StringType
    "boolean" -> BoolType
    _ -> error "Unknown column type"

convertRows :: [[Y.Value]] -> [Row]
convertRows = Data.List.map (Data.List.map convertValue)

getStatementType :: String -> StatementType
getStatementType query 
    | "select" `isPrefixOf` lowerQuery = if "show" `isPrefixOf` lowerQuery then ShowTable else Select
    | "insert" `isPrefixOf` lowerQuery = Insert
    | "update" `isPrefixOf` lowerQuery = Update
    | "delete" `isPrefixOf` lowerQuery = Delete
    | "show tables" `Data.List.isInfixOf` lowerQuery = ShowTables
    | "show table" `isPrefixOf` lowerQuery = ShowTable
    | otherwise = InvalidStatement
  where
    lowerQuery = map toLower query

convertValue :: Y.Value -> DataFrame.Value
convertValue val = case val of
    Y.String s -> DataFrame.StringValue (T.unpack s)
    Y.Number n -> DataFrame.IntegerValue (round n)
    Y.Bool b -> DataFrame.BoolValue b
    Y.Null -> DataFrame.NullValue
    _ -> error "Unsupported value type"

serializeTableToYAML :: SerializedTable -> String
serializeTableToYAML st =
    "tableName: " Data.List.++ tableName st Data.List.++ "\n" Data.List.++
    "columns:\n" Data.List.++ Data.List.concatMap serializeColumn (columns st) Data.List.++
    "rows:\n" Data.List.++ Data.List.concatMap serializeRow (rows st)
  where
    serializeColumn :: SerializedColumn -> String
    serializeColumn col =
        "- name: " Data.List.++ name col Data.List.++ "\n" Data.List.++
        "  dataType: " Data.List.++ dataType col Data.List.++ "\n"

    serializeRow :: [Y.Value] -> String
    serializeRow row = "- [" Data.List.++ Data.List.intercalate ", " (Data.List.map serializeValue row) Data.List.++ "]\n"

    serializeValue :: Y.Value -> String
    serializeValue val =
        case val of
            Y.String s -> T.unpack s
            Y.Number n -> show (round n :: Int)
            Y.Bool b   -> Data.List.map Data.Char.toLower (show b)
            Y.Null     -> "null"
            _          -> "ERROR"

dataFrameToSerializedTable :: (TableName, DataFrame) -> SerializedTable
dataFrameToSerializedTable (tblName, DataFrame columns rows) =
    SerializedTable {
        tableName = tblName,
        columns = Data.List.map convertColumnToSerializedColumn columns,
        rows = Data.List.map convertRow rows
    }
  where
    convertColumnToSerializedColumn :: Column -> SerializedColumn
    convertColumnToSerializedColumn (Column name columnType) =
        SerializedColumn {
            name = name,
            dataType = convertColumnTypeToString columnType
        }

    convertColumnTypeToString :: ColumnType -> String
    convertColumnTypeToString IntegerType = "integer"
    convertColumnTypeToString StringType = "string"
    convertColumnTypeToString BoolType = "boolean"

    convertRow :: Row -> [Y.Value]
    convertRow = Data.List.map convertValueToYValue

    convertValueToYValue :: Value -> Y.Value
    convertValueToYValue (IntegerValue n) = Y.Number (fromIntegral n)
    convertValueToYValue (StringValue s) = Y.String (T.pack s)
    convertValueToYValue (BoolValue b) = Y.Bool b
    convertValueToYValue NullValue = Y.Null


-- =======================================
-- Statement validation


validateStatement :: ParsedStatement -> [(TableName, DataFrame)] -> (Bool, ErrorMessage)
validateStatement stmt tables = case stmt of
  SelectAll tableNames whereClause -> returnError $ Data.List.all (`Data.List.elem` Data.List.map fst tables) tableNames && validateWhereClause whereClause tables
  SelectColumns tableNames cols whereClause -> returnError $ validateTableAndColumns tableNames (Just cols) tables && validateWhereClause whereClause tables
  SelectAggregate tableNames cols whereClause -> returnError $ validateTableAndColumns tableNames (Just cols) tables && validateWhereClause whereClause tables
  InsertStatement tableName cols vals -> returnError $ validateTableAndColumns [tableName] cols tables && Data.List.all (\(column, value) -> selectColumnMatchesValue column tables value) (Data.List.zip (fromMaybe [] cols) vals)
  UpdateStatement tableName cols vals whereClause -> returnError $ validateTableAndColumns [tableName] (Just cols) tables && validateWhereClause whereClause tables && Data.List.all (\(column, value) -> selectColumnMatchesValue column tables value) (Data.List.zip cols vals)
  DeleteStatement tableName whereClause -> returnError $ tableName `Data.List.elem` Data.List.map fst tables && validateWhereClause whereClause tables
  ShowTablesStatement -> returnError True
  ShowTableStatement tableName -> returnError $ Data.List.elem tableName $ Data.List.map fst tables
  Invalid err -> (False, err)

returnError :: Bool -> (Bool, ErrorMessage)
returnError bool = (bool, "Non existant columns or tables in statement or values dont match column")

validateWhereClause :: Maybe WhereClause -> [(TableName, DataFrame)] -> Bool
validateWhereClause clause tables = case clause of
  Just cClause -> validateExistingClause cClause tables
  Nothing -> True

validateExistingClause :: WhereClause -> [(TableName, DataFrame)] -> Bool
validateExistingClause (IsValueBool _ tableName columnName ) tables = tablesExist [tableName] tables && colInDf && columnIsBool
  where
    df = fromMaybe (DataFrame [] []) (Data.List.lookup tableName tables)
    colInDf = columnExistsInDataFrame (TableColumn tableName columnName) df
    columnIsBool = getColumnType (getColumnByName columnName (columnsList df)) == BoolType

validateExistingClause (Conditions conditions) tables = Data.List.all validCondition conditions
  where
    validCondition :: Condition -> Bool
    validCondition (Equals column val) = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tables) && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (LessThan column val) = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tables) && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (GreaterThan column val) = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tables) && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (LessthanOrEqual column val) = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tables) && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (GreaterThanOrEqual column val) = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tables) && selectColumnMatchesValue column tables (getValueFromConditionValue val)
    validCondition (NotEqual column val) = maybe False (columnsExistInTable [column]) (Data.List.lookup (getTableNameFromColumn column) tables) && selectColumnMatchesValue column tables (getValueFromConditionValue val)

selectColumnMatchesValue :: SelectColumn -> [(TableName, DataFrame)] -> Value -> Bool
selectColumnMatchesValue col@(TableColumn tableName columnName) tables val = tableExists && isMatchingValue
  where
    tableExists = validateTableAndColumns [tableName] (Just [col]) tables
    columns = getTableColumns tables tableName
    column = getColumnByName columnName columns
    isMatchingValue = columnMatchesValue column val

selectColumnMatchesValue _ _ _ = False

columnsList :: DataFrame -> [Column]
columnsList (DataFrame cols _) = cols

getColumnByName :: String -> [Column] -> Column
getColumnByName name cols = fromMaybe (Column "notfound" BoolType) (Data.List.find (\(Column colName _) -> colName == name) cols)


getValueFromConditionValue :: ConditionValue -> Value
getValueFromConditionValue (StrValue str) = StringValue str
getValueFromConditionValue (IntValue num) = IntegerValue num

columnMatchesValue :: Column -> Value -> Bool
columnMatchesValue (Column _ t) value = isNothing maybeValueType || (isJust maybeValueType && fromMaybe IntegerType maybeValueType == t )
  where
    maybeValueType = getColumnTypeFromValueType value

getTableNameFromColumn :: SelectColumn -> TableName
getTableNameFromColumn (TableColumn tname _) = tname
getTableNameFromColumn (Avg tname _) = tname
getTableNameFromColumn (Max tname _) = tname
getTableNameFromColumn Now = "NO TABLE"

getColumnTypeFromValueType :: Value -> Maybe ColumnType
getColumnTypeFromValueType (IntegerValue _) = Just IntegerType
getColumnTypeFromValueType (StringValue _) = Just StringType
getColumnTypeFromValueType (BoolValue _) = Just BoolType
getColumnTypeFromValueType NullValue = Nothing

getColumnType :: Column -> ColumnType
getColumnType (Column _ columnType) = columnType

tablesExist :: [TableName] -> [(TableName, DataFrame)] -> Bool
tablesExist tables databaseTables = Data.List.all (`Data.List.elem` tablesInDatabase) tables
  where tablesInDatabase = Data.List.map fst databaseTables

validateTableAndColumns :: [TableName] -> Maybe [SelectColumn] -> [(TableName, DataFrame)] -> Bool
validateTableAndColumns tableNames cols tables = isJust cols && Data.List.all tableAndColumnsExist tableNames
  where
    justCols = fromMaybe [] cols
    tableAndColumnsExist tableName = maybe False (columnsExistInTable justCols) (Data.List.lookup tableName tables)

columnsExistInTable :: [SelectColumn] -> DataFrame -> Bool
columnsExistInTable columns df = Data.List.all (`columnExistsInDataFrame` df) columns

columnExistsInDataFrame :: SelectColumn -> DataFrame -> Bool
columnExistsInDataFrame (TableColumn _ colName) (DataFrame cols _) =
  Data.List.any (\(Column name _) -> name == colName) cols

columnExistsInDataFrame (Avg _ colName) (DataFrame cols _) =
  Data.List.any (\(Column name t) -> name == colName && t == IntegerType) cols

columnExistsInDataFrame (Max _ colName) (DataFrame cols _) =
  Data.List.any (\(Column name _) -> name == colName) cols

columnExistsInDataFrame Now _ = True


-- ============================
-- End of StatementValidation


getSelectedColumns :: Lib3.ParsedStatement -> [(Lib3.TableName, DataFrame)] -> [Column]
getSelectedColumns stmt tbls = case stmt of
    Lib3.SelectAll tableNames _ -> Data.List.concatMap (getTableColumns tbls) tableNames
    Lib3.SelectColumns _ selectedColumns _ -> mapMaybe (findColumn tbls) selectedColumns
    _ -> []

getTableColumns :: [(Lib3.TableName, DataFrame)] -> Lib3.TableName -> [Column]
getTableColumns tbls tableName = case Data.List.lookup tableName tbls of
    Just (DataFrame columns _) -> columns
    Nothing -> []

findColumn :: [(Lib3.TableName, DataFrame)] -> Lib3.SelectColumn -> Maybe Column
findColumn tbls (Lib3.TableColumn tblName colName) =
    case Data.List.lookup tblName tbls of
        Just (DataFrame columns _) -> Data.List.find (\(Column name _) -> name == colName) columns
        Nothing -> Nothing
findColumn _ _ = Nothing

-- ONLY SQL PARSER BELOW

parseStatement :: String  -> Either ErrorMessage ParsedStatement
parseStatement input
  | Data.List.last input /= ';' = Left "Missing semicolon at end of statement"
  | otherwise = mapStatementType wordsInput
  where
    cleanedInput = Data.List.init input
    wordsInput = parseSemiCaseSensitive cleanedInput

mapStatementType :: [String] -> Either ErrorMessage ParsedStatement
mapStatementType statement = do
  statementType <- guessStatementType statement
  case statementType of
      Select -> do
        parseSelect (Data.List.drop 1 statement)
      Insert -> do
        parseInsert statement
      Delete -> do
        parseDelete statement
      Update -> do
        parseUpdate statement
      ShowTable -> do
        parseShowTable statement
      ShowTables -> do
        parseShowTables statement
      InvalidStatement -> do
        Left "Invalid statement type"

guessStatementType :: [String] -> Either ErrorMessage StatementType
guessStatementType (x : y : _)
  | x == "select" = Right Select
  | x == "insert" && y == "into" = Right Insert
  | x == "delete" && y == "from" = Right Delete
  | x == "update" = Right Update
  | x == "show" && y == "table" = Right ShowTable
  | x == "show" && y == "tables" = Right ShowTables
  | otherwise = Left "Sql statement does not resemble any sort of implemented statement"

guessStatementType _ = Left "Sql statement does not resemble any sort of implemented statement"

parseInsert :: [String] -> Either ErrorMessage ParsedStatement
parseInsert statement = do
  tableName <- getInsertTableName statement
  getInsertColumns statement tableName

getInsertTableName :: [String] -> Either ErrorMessage TableName
getInsertTableName statement = do
  (_, intoAndAfter) <- Right $ Data.List.break (== "into") statement
  tableToInsertInto <- Right $ Data.List.drop 1 intoAndAfter
  if not (Data.List.null tableToInsertInto) && isValidTable (Data.List.head tableToInsertInto)
      then Right $ Data.List.head tableToInsertInto
      else Left "Update statement table name does not meet requirements. Maybe ilegal characters were used?"

getInsertColumns :: [String] -> TableName -> Either ErrorMessage ParsedStatement
getInsertColumns statement tableName = do
  (_, intoAndAfter) <- Right $ Data.List.break (== "into") statement
  (intoToValues, valuesAndAfter) <- Right $ Data.List.break (== "values") intoAndAfter
  cleanedValues <- cleanInsertHeadAndTail (Data.List.drop 1 valuesAndAfter)
  columnNames <- getColumnNames $ Data.List.drop 2 intoToValues
  columnValues <- getInsertColumnValues cleanedValues
  columns <-
    if Data.List.length columnNames /= Data.List.length columnValues
      then Left "Column count does not match value count in insert statement"
      else Right $ Data.List.map (TableColumn tableName) columnNames
  getInsertStatement tableName columns columnValues

getInsertStatement :: TableName -> [SelectColumn] -> [Value] -> Either ErrorMessage ParsedStatement
getInsertStatement tableName columns values = Right $ InsertStatement tableName (Just columns)  values

-- valueToString :: Value -> String
-- valueToString (IntegerValue i) = show i
-- valueToString (StringValue s) = s
-- valueToString (BoolValue b) = show b
-- valueToString NullValue = "null"

getInsertColumnValues :: [String] -> Either ErrorMessage [Value]
getInsertColumnValues [value] = do
  val <- getValueFromString value
  return [val]

getInsertColumnValues (value : xs) = do
  cleanedString <- if Data.List.last value == ',' then Right $ Data.List.init value else Left "Failed to parse INSERT values"
  val <- getValueFromString cleanedString
  rest <- getInsertColumnValues xs
  return $ val : rest

getInsertColumnValues _ = Left "Error parsing insert statement values"

getColumnNames :: [String] -> Either ErrorMessage [String]
getColumnNames rawNames = do
  cleanedHeadAndTail <- cleanInsertHeadAndTail rawNames
  cleanInsertCommas cleanedHeadAndTail

cleanInsertHeadAndTail :: [String] -> Either ErrorMessage [String]
cleanInsertHeadAndTail input =
  if not (Data.List.null input) && Data.List.head (Data.List.head input) == '(' && Data.List.last (Data.List.last input) == ')'
    then
      if Data.List.length input == 1
        then Right [Data.List.drop 1 (Data.List.init (Data.List.head input))]
        else Right $ [Data.List.drop 1 (Data.List.head input)] Data.List.++ Data.List.init (Data.List.drop 1 input) Data.List.++ [Data.List.init (Data.List.last input)]
    else Left "formating of insert statement does not meet requirements. Most likely missing `(` or `)`"

cleanInsertCommas :: [String] -> Either ErrorMessage [String]
cleanInsertCommas [column] =
  if isValidColumnWithoutAb column
    then Right [column]
    else Left $ "Column" Data.List.++ " `" Data.List.++ column Data.List.++ "` " Data.List.++ "contains ilegal characters"

cleanInsertCommas (column : xs) = do
  currentColumn <-
    if Data.List.last column == ',' && isValidColumnWithoutAb column
      then Right $ Data.List.init column
      else Left "Missing comma or invalid column name for insert statement"
  rest <- cleanInsertCommas xs
  return $ currentColumn : rest

cleanInsertCommas _ = Left "Unknown error parsing columns in insert statement"

parseDelete :: [String] -> Either ErrorMessage ParsedStatement
parseDelete statement = do
  tableName <- getTableNameFromDelete statement
  (_, fromWhere) <- Right $ Data.List.break (== "where") statement
  whereClause <- statementClause' fromWhere tableName
  getDeleteStatement tableName whereClause

getDeleteStatement :: TableName -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
getDeleteStatement tableName whereClause = Right $ DeleteStatement tableName whereClause

getTableNameFromDelete :: [String] -> Either ErrorMessage TableName
getTableNameFromDelete statement = do
  (_, fromAndElse) <- Right $ Data.List.break (== "from") statement
  (fromToWhere, _) <- Right $ Data.List.break (== "where") fromAndElse
  if Data.List.length fromToWhere /= 2 || not (isValidTable (Data.List.last fromToWhere))
      then Left $ "Invalid delete statement table name: " Data.List.++ Data.List.last fromToWhere
      else Right $ Data.List.last fromToWhere


parseUpdate :: [String] -> Either ErrorMessage ParsedStatement
parseUpdate statement = do
  (_, afterWhere) <- Right $ Data.List.break (== "where") statement
  tableName <- getUpdateTableName statement
  columnAndNewValueString <- getColumnAndNewValueStringsForUpdate statement
  selectedColumns <- getSelectedColumnsForUpdate columnAndNewValueString tableName
  updateValues <- getValuesToUpdate columnAndNewValueString
  whereClause <- statementClause' afterWhere tableName
  getUpdateStatement tableName selectedColumns updateValues whereClause

-- Why did I use `do` here? Well... Thats a good question that I don't know the answer to
getUpdateTableName :: [String] -> Either ErrorMessage TableName
getUpdateTableName statement = do
  (beforeSet, _) <- Right $ Data.List.break (== "set") statement
  tableToUpdate <- Right $ Data.List.drop 1 beforeSet
  if isUpdateTableNameValid tableToUpdate
      then Right $ Data.List.head tableToUpdate
      else Left "Update statement table name does not meet requirements. Maybe ilegal characters were used?"

isUpdateTableNameValid :: [String] -> Bool
isUpdateTableNameValid table = Data.List.length table == 1 && isValidTable (Data.List.head table)

getColumnAndNewValueStringsForUpdate :: [String] -> Either ErrorMessage [String]
getColumnAndNewValueStringsForUpdate updateStatement = do
  (_, setAndAfter) <- Right $ Data.List.break (== "set") updateStatement
  (fromSetToWhere, _) <- Right $ Data.List.break (== "where") setAndAfter
  cleanedFromSetToWhere <- Right $ cleanLastComma fromSetToWhere
  Right $ Data.List.drop 1 cleanedFromSetToWhere

cleanLastComma :: [String] -> [String]
cleanLastComma = Data.List.map (\str -> if Data.List.last str == ',' then Data.List.init str else str)

getSelectedColumnsForUpdate :: [String] -> TableName -> Either ErrorMessage [SelectColumn]
getSelectedColumnsForUpdate [val1, op, val2] tableName = do
  baseColumn <- getValidUpdateColumn val1 op val2 tableName
  return [baseColumn]

getSelectedColumnsForUpdate (val1 : op : val2 : xs) tableName = do
  baseColumn <- getValidUpdateColumn val1 op val2 tableName
  rest <- getSelectedColumnsForUpdate xs tableName
  return $ rest Data.List.++ [baseColumn]

getSelectedColumnsForUpdate _ _ = Left "Invalid column formating for UPDATE statement"

getValidUpdateColumn :: String -> String -> String -> TableName -> Either ErrorMessage SelectColumn
getValidUpdateColumn val1 op val2 tableName
  | op == "=" && checkIfDoesNotContainSpecialSymbols val1 && (isNumber val2 || val2 == "true" || val2 == "false" || ("'" `Data.List.isPrefixOf` val2 && "'" `Data.List.isSuffixOf` val2 )) = Right $ TableColumn tableName val1
  | otherwise = Left "Unable to update table do to bad update statement. Failed to parse new column values"

checkIfDoesNotContainSpecialSymbols :: String -> Bool
checkIfDoesNotContainSpecialSymbols val1 = Data.List.notElem '\'' val1 && Data.List.notElem '.' val1 && Data.List.notElem '(' val1 && Data.List.notElem ')' val1

getValuesToUpdate :: [String] -> Either ErrorMessage [Value]
getValuesToUpdate [val1, op, val2] = do
  baseColumn <- getValidUpdateValue val1 op val2
  return [baseColumn]

getValuesToUpdate (val1 : op : val2 : xs) = do
  baseColumn <- getValidUpdateValue val1 op val2
  rest <- getValuesToUpdate xs
  return $ rest Data.List.++ [baseColumn]

getValuesToUpdate _  = Left "Invalid column formating for UPDATE statement"

getValidUpdateValue :: String -> String -> String -> Either ErrorMessage Value
getValidUpdateValue val1 op val2
  | op == "=" && checkIfDoesNotContainSpecialSymbols val1 && (isNumber val2 || val2 == "true" || val2 == "false" || ("'" `Data.List.isPrefixOf` val2 && "'" `Data.List.isSuffixOf` val2 )) = getValueFromString val2
  | otherwise = Left "Unable to update table do to bad update statement. Failed to parse columns to be updated"

getValueFromString :: String -> Either ErrorMessage Value
getValueFromString valueString
  | valueString == "true" = Right $ BoolValue True
  | valueString == "false" = Right $ BoolValue False
  | isNumber valueString = Right $ IntegerValue (read valueString :: Integer)
  | "'" `Data.List.isPrefixOf` valueString && "'" `Data.List.isSuffixOf` valueString = Right $ StringValue $ Data.List.drop 1 (Data.List.init valueString)
  | otherwise = Left "Failed to parse UPDATE statement value. Only string, integer and bool values allowed"

getUpdateStatement :: TableName -> [SelectColumn] -> Row -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
getUpdateStatement tableName selectedColumns rows whereClause = Right $ UpdateStatement tableName selectedColumns rows whereClause


parseShowTables :: [String] -> Either ErrorMessage ParsedStatement
parseShowTables ["show", "tables"] = Right ShowTablesStatement
parseShowTables _ = Left "Failed to parse SHOW TABLES statement"

parseShowTable :: [String] -> Either ErrorMessage ParsedStatement
parseShowTable ["show", "table", tableName] = do
  if isValidTable tableName
    then do
      return $ ShowTableStatement tableName
    else do
      Left $ "SHOW TABLE table name contains ilegal characters"Data.List.++ " " Data.List.++ tableName

parseShowTable _ = Left "Failed to parse SHOW TABLE statement"

isValidTable :: String -> Bool
isValidTable originalName
  | "'" `Data.List.isPrefixOf` originalName || "'" `Data.List.isSuffixOf` originalName || "avg(" `Data.List.isPrefixOf` originalName || "max(" `Data.List.isPrefixOf` originalName || ")" `Data.List.isSuffixOf` originalName = False
  | otherwise = True

parseSelect :: [String] -> Either ErrorMessage ParsedStatement
parseSelect statement = do
  tableNamesAndAb <- getTableNamesAndAb statement
  (beforeWhere, afterWhere) <- Right $ Data.List.break (== "where") statement
  (beforeFrom, _) <- Right $ Data.List.break (== "from") beforeWhere
  whereClause <- statementClause afterWhere tableNamesAndAb
  selectType <- getSelectType beforeFrom
  tableNames <- Right $ Data.List.map fst tableNamesAndAb
  case selectType of
    Aggregate -> do
      aggregateColumns <- getAggregateColumns beforeFrom tableNamesAndAb
      parseAggregate tableNames whereClause aggregateColumns
    ColumnsAndTime -> do
      columnsWithTableAb <- getColumnWithTableAb statement tableNamesAndAb
      parseColumnsSelect tableNames columnsWithTableAb whereClause
    AllColumns -> do
      parseAllColumns tableNames whereClause

castEither :: a -> Either ErrorMessage a -> a
castEither defaultValue eitherValue = case eitherValue of
  Right val -> val
  Left _ -> defaultValue

getAggregateColumns :: [String] -> [(TableName, String)] -> Either ErrorMessage [SelectColumn]
getAggregateColumns [baseColumn] tableNames = do
  parsedAggregate <- parseAggregateColumn baseColumn tableNames
  return [parsedAggregate]
getAggregateColumns (baseColumn : xs) tableNames = do
  baseAggregate <- parseAggregateColumn baseColumn tableNames
  rest <- getAggregateColumns xs tableNames
  return $ baseAggregate : rest
getAggregateColumns _ _ = Left "Error parsing aggregate columns"

parseAggregateColumn :: String -> [(TableName, String)] -> Either ErrorMessage SelectColumn
parseAggregateColumn column tableNames
  | isValid && "avg(" `Data.List.isPrefixOf` dropedCommaColumn && ")" `Data.List.isSuffixOf` dropedCommaColumn = Right $ Avg tableName columnName
  | isValid && "max(" `Data.List.isPrefixOf` dropedCommaColumn && ")" `Data.List.isSuffixOf` dropedCommaColumn = Right $ Max tableName columnName
  | otherwise = Left $ "Failed to parse aggregate column " Data.List.++ column
    where
      dropedCommaColumn = if Data.List.last column == ',' then Data.List.init column else column
      removedAggregate = if Data.List.last column == ',' then Data.List.init (Data.List.drop 4 (Data.List.init column)) else Data.List.drop 4 (Data.List.init column)
      [tableAb, columnName] = wordsWhen (== '.') removedAggregate
      isValid = findIfTupleWithSndElemEqualToExists tableAb tableNames
      tableName = castEither "Upsi" $ getFstTupleElemBySndElemInList (tableAb, columnName) tableNames


findIfTupleWithSndElemEqualToExists :: Eq a => a -> [(b, a)] -> Bool
findIfTupleWithSndElemEqualToExists _ [] = False;
findIfTupleWithSndElemEqualToExists val [(_, sndVal)] = val == sndVal
findIfTupleWithSndElemEqualToExists val ((_, sndVal) : xs) = (val == sndVal) || findIfTupleWithSndElemEqualToExists val xs


getTableNamesAndAb :: [String] -> Either ErrorMessage [(TableName, String)]
getTableNamesAndAb statement = names
  where
    (_, afterFrom) = Data.List.break (== "from") statement
    (tables, _) = Data.List.break (== "where") afterFrom
    dropedTable = Data.List.drop 1 tables
    len = Data.List.length dropedTable
    abbs = Data.List.map snd (fst (Data.List.partition (odd . fst) (Data.List.zip [0 .. ] dropedTable)))
    tuples = splitIntoTuples dropedTable

    tuplesWithoutCommaAtEndOfAbb = case tuples of
      Right tupleList -> Right $ Data.List.map (\(tableName, tableAb) -> if Data.List.last tableAb == ',' then (tableName, Data.List.init tableAb) else (tableName, tableAb)) tupleList
      Left err -> Left err
    names
      | even len && len > 0 && valuesListLike abbs = tuplesWithoutCommaAtEndOfAbb
      | otherwise = Left "Invalid table formating in statement. Maybe table abbreviation was not provided?"

valuesListLike :: [String] -> Bool
valuesListLike [val] = Data.List.last val /= ','
valuesListLike (x : xs) = Data.List.last x == ',' && valuesListLike xs
valuesListLike [] = False

getColumnWithTableAb :: [String] -> [(TableName, String)] -> Either ErrorMessage [(String, String)]
getColumnWithTableAb statement tableNames = do
  (beforeFrom, _) <- Right $ Data.List.break (== "from") statement
  columnAndAbList <- Right $ getListOfTableAbAndColumn beforeFrom
  isValidColumnNames <- Right $ Data.List.all isValidColumn beforeFrom
  if Data.List.null columnAndAbList || odd (Data.List.length columnAndAbList) || not isValidColumnNames
    then do
      Left "error parsing columns. Maybe table name abbreviation was not provided?"
    else do
      splitList <- splitIntoTuples columnAndAbList
      getCorrectTableAndColumns splitList tableNames

getCorrectTableAndColumns :: [(String, String)] -> [(String, String)] -> Either ErrorMessage [(String, String)]

getCorrectTableAndColumns [columnAndTableAb] tableNames = do
  tableName <- getFstTupleElemBySndElemInList columnAndTableAb tableNames
  return [(tableName, snd columnAndTableAb)]

getCorrectTableAndColumns (columnAndTableAb : xs) tableNames = do
  tableExists <- Right $ findIfTupleWithSndElemEqualToExists (fst columnAndTableAb) tableNames
  if tableExists
    then do
      tableName <- getFstTupleElemBySndElemInList columnAndTableAb tableNames
      rest <- getCorrectTableAndColumns xs tableNames
      return $ (tableName, snd columnAndTableAb) : rest
    else
      Left "Error parsing column table abbreviations"

getCorrectTableAndColumns _ _ = Left "Something went wrong when trying to find match for table abbreviation"

getFstTupleElemBySndElemInList :: Eq a => (a, b) -> [(c, a)] -> Either ErrorMessage c

getFstTupleElemBySndElemInList  (val1, _) [(val3, val2)] = if val1 == val2 then Right val3 else Left "Failed to find table name that matches one of the abbriviations"

getFstTupleElemBySndElemInList val1 (x : xs) = do
  let isMatch (src, _) (_, src') = src == src'
  isFirstMatch <- Right $ isMatch val1 x
  if isFirstMatch
    then Right $ fst x
    else getFstTupleElemBySndElemInList val1 xs

getFstTupleElemBySndElemInList _ _ = Left "Failed to find valid table that matches table abbreviation"


getListOfTableAbAndColumn :: [String] -> [String]
getListOfTableAbAndColumn [] = []
getListOfTableAbAndColumn [abAndColumn] = wordsWhen (== '.') abAndColumn
getListOfTableAbAndColumn (abAndColumn : xs) = wordsWhen (== '.') abAndColumn Data.List.++ getListOfTableAbAndColumn xs


wordsWhen :: (Char -> Bool) -> String -> [String]
wordsWhen p s = case Data.List.dropWhile p s of
  "" -> []
  s' -> w : wordsWhen p s''
        where (w, s'') = Data.List.break p s'


splitIntoTuples :: [String] -> Either ErrorMessage [(String, String)]
splitIntoTuples [x, y] = Right [(x, y)]
splitIntoTuples (x : y : xs) = do
  baseTuple <- Right (x, y)
  rest <- splitIntoTuples xs
  return $ baseTuple : rest

splitIntoTuples _ = Left "Error parsing tables or columns. Maybe table abbreviation was not provided?"

parseAggregate :: [TableName] -> Maybe WhereClause -> [SelectColumn] -> Either ErrorMessage ParsedStatement
parseAggregate tableNames whereClause columns = Right $ SelectAggregate tableNames columns whereClause

-- ignores commas in columns
parseColumnsSelect :: [TableName] -> [(String, String)] -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
parseColumnsSelect tables columns whereClause = Right $ SelectColumns tables (Data.List.map (uncurry TableColumn) columns) whereClause

parseAllColumns :: [TableName] -> Maybe WhereClause -> Either ErrorMessage ParsedStatement
parseAllColumns tableNames whereClause = Right $ SelectAll tableNames whereClause

getSelectType :: [String] -> Either ErrorMessage SelectType
getSelectType selectColumns
  | hasAllValidAggregates selectColumns = Right Aggregate
  | hasAllValidColumns selectColumns = Right ColumnsAndTime
  | isValidSelectAll selectColumns = Right AllColumns
  | otherwise = Left "Error parsing select type. Maybe invalid table abbreviation was used or an invalid query was given"

isValidSelectAll :: [String] -> Bool
isValidSelectAll selectColumns
  | Data.List.length selectColumns == 1 && Data.List.head selectColumns == "*" = True
  | otherwise = False

hasAllValidColumns :: [String] -> Bool
hasAllValidColumns [] = False
hasAllValidColumns [column] = isValidColumn column
hasAllValidColumns (column : xs) = isValidColumn column && hasAllValidColumns xs

isValidColumn :: String -> Bool
isValidColumn column = isSplitByDot && isNotAggregate && hasNoParenthesies || column == "now()"
  where
    (beforeDot, afterDot) = Data.List.break (== '.') column
    isSplitByDot = not (Data.List.null beforeDot) && not (Data.List.null afterDot) && (Data.List.length afterDot > 1)
    isNotAggregate = not $ isAggregate column
    hasNoParenthesies = not ("'" `Data.List.isPrefixOf` column || "'" `Data.List.isSuffixOf` column)

isValidColumnWithoutAb :: String -> Bool
isValidColumnWithoutAb column = isNotAggregate && hasNoParenthesies || column == "now()"
  where
    isNotAggregate = not $ "avg(" `Data.List.isPrefixOf` column || "max(" `Data.List.isPrefixOf` column || "'" `Data.List.isSuffixOf` column
    hasNoParenthesies = not ("'" `Data.List.isPrefixOf` column || "'" `Data.List.isSuffixOf` column)

hasAllValidAggregates :: [String] -> Bool
hasAllValidAggregates [] = False
hasAllValidAggregates [column] = isAggregate column
hasAllValidAggregates (column : xs) = isAggregate column && hasAllValidAggregates xs

isAggregate :: String -> Bool
isAggregate column
  | ("max(" `Data.List.isPrefixOf` removedCommaColumn || "avg(" `Data.List.isPrefixOf` removedCommaColumn) && ")" `Data.List.isSuffixOf` removedCommaColumn && isValidColumn (Data.List.drop 4 (Data.List.init removedCommaColumn)) = True
  | otherwise = False
    where
      removedCommaColumn = if Data.List.last column == ',' then Data.List.init column else column
statementClause :: [String] -> [(TableName, String)] -> Either ErrorMessage (Maybe WhereClause)
statementClause afterWhere tableNames = do
  case Data.List.length afterWhere of
    0 -> do
      Right Nothing
    _ -> do
      tryParseWhereClause afterWhere tableNames

tryParseWhereClause :: [String] -> [(TableName, String)] -> Either ErrorMessage (Maybe WhereClause)
tryParseWhereClause afterWhere tableNames
  | isBoolIsTrueFalseClauseLike = case splitStatementToWhereIsClause (Data.List.drop 1 afterWhere) tableNames of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | isAndClauseLike = case parseWhereAnd (Data.List.drop 1 afterWhere) tableNames of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | otherwise = Left "Failed to parse where clause. Where clause type not implemented or recognised. Please only use `where and` and `where bool is true/false`"
  where
    afterWhereWithoutWhere = Data.List.drop 1 afterWhere
    (_, afterIs) = Data.List.break (== "is") afterWhereWithoutWhere
    firstElementIsColumn = not (Data.List.null afterWhereWithoutWhere) && (Data.List.length (wordsWhen (== '.') (Data.List.head afterWhereWithoutWhere)) == 2)
    isBoolIsTrueFalseClauseLike = Data.List.length afterWhereWithoutWhere == 3 && Data.List.length afterIs == 2 && (afterIs Data.List.!! 1 == "false" || afterIs Data.List.!! 1 == "true") && firstElementIsColumn
    isAndClauseLike = Data.List.null afterIs

splitStatementToWhereIsClause :: [String] -> [(TableName, String)] -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause [boolColName, "is", boolString] tableNames = do
   validTableName <- getFstTupleElemBySndElemInList (tableName, colName) tableNames
   Right $ IsValueBool parsedBoolString validTableName $ Data.List.drop 1 colName
  where
    (tableName, colName) = Data.List.break (== '.') boolColName
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause _ _ = Left "Unsupported or invalid where bool is true false clause. Maybe the formating is wrong?"

parseWhereAnd :: [String] -> [(TableName, String)] -> Either ErrorMessage WhereClause
parseWhereAnd afterWhere tableNames
  | matchesWhereAndPattern afterWhere = splitStatementToAndClause afterWhere
  | otherwise = Left "Unable to parse WHERE AND clause"
  where
    splitStatementToAndClause :: [String] -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList = do
      conditionList <- getConditionList strList
      getConditions conditionList

    getConditions :: [Condition] -> Either ErrorMessage WhereClause
    getConditions conditions = Right $ Conditions conditions

    getConditionList :: [String] -> Either ErrorMessage [Condition]
    getConditionList [condition1, operator, condition2] = do
      condition <- getCondition condition1 operator condition2 tableNames
      return [condition]

    getConditionList (condition1 : operator : condition2 : "and" : xs) = do
      conditionBase <- getCondition condition1 operator condition2 tableNames
      rest <- if not (Data.List.null xs) then getConditionList xs else Right []
      return $ conditionBase : rest

    getConditionList _ = Left "Error parsing where and clause"

getConditionValue :: String -> Either ErrorMessage ConditionValue
getConditionValue condition
  | isNumber condition = Right $ IntValue (read condition :: Integer)
  | Data.List.length condition > 2 && "'" `Data.List.isPrefixOf` condition && "'" `Data.List.isSuffixOf` condition = Right $ StrValue (Data.List.drop 1 (Data.List.init condition))
  | otherwise = Left "Error parsing condition value"

getCondition :: String -> String -> String -> [(TableName, String)] -> Either ErrorMessage Condition
getCondition val1 op val2 tableNames
  | op == "=" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ Equals (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "=" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1= Right $ Equals (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == ">" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThan (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == ">" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThan (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessThan (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessThan (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == ">=" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThanOrEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == ">=" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThanOrEqual (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<=" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessthanOrEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<=" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessthanOrEqual (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | op == "<>" && isRight val1TableEither && val1HasDot && isJust value2Type && isNothing value1Type && isCondition2 = Right $ NotEqual (TableColumn val1Table (Data.List.drop 1 val1Column)) $ castEither defaultCondition condition2
  | op == "<>" && isRight val2TableEither && val2HasDot && isJust value1Type && isNothing value2Type && isCondition1 = Right $ NotEqual (TableColumn val2Table (Data.List.drop 1 val2Column)) $ castEither defaultCondition condition1
  | isLeft val2TableEither && isLeft val1TableEither = Left "Abbreviation in WHERE clause does not match any table in FROM part of statement"
  | otherwise = Left "Error parsing where and condition. Only able to compare integer and string values with columns"
  where
    (val1TableAb, val1Column) = Data.List.break (=='.') val1
    (val2TableAb, val2Column) = Data.List.break (=='.') val2
    value1Type = parseType val1
    value2Type = parseType val2
    val1HasDot = not (Data.List.null (Data.List.drop 1 val1Column))
    val2HasDot = not (Data.List.null (Data.List.drop 1 val2Column))
    condition1 = getConditionValue val1
    condition2 = getConditionValue val2
    val1TableEither  = getFstTupleElemBySndElemInList (val1TableAb, "I LOVE HAKELL") tableNames
    val2TableEither  = getFstTupleElemBySndElemInList (val2TableAb, "THIS CODE I SO GREAT. ITS THE BEST CODE. THE BEST") tableNames

    val1Table = castEither "If you are the user of this app and you are seeing this. I am sorry to inform you that the creators of this program suck at coding" val1TableEither
    val2Table = castEither "If you are the user of this app and you are seeing this. I am sorry to inform you that the creators of this program suck at coding" val2TableEither

    defaultCondition = StrValue "Kas skaitys tas gaidys (Isskyrus destytoja)"

    isCondition1 = case condition1 of
      Right _ -> True
      Left _ -> False

    isCondition2 = case condition2 of
      Right _ -> True
      Left _ -> False


matchesWhereAndPattern :: [String] -> Bool
matchesWhereAndPattern [condition1, operator, condition2] = isWhereAndOperation condition1 operator condition2
matchesWhereAndPattern (condition1 : operator : condition2 : andString : xs) = matchesWhereAndPattern [condition1, operator, condition2] && andString == "and" && matchesWhereAndPattern xs
matchesWhereAndPattern _ = False

isWhereAndOperation :: String -> String -> String -> Bool
isWhereAndOperation condition1 operator condition2
  | Data.List.elem operator [">", "<", "=", "<>", "<=", ">="] && (col1Valid || col2Valid) = True
  | otherwise = False
  where
    (val1Table, val1Column) = Data.List.break (== '.') condition1
    (val2Table, val2Column) = Data.List.break (== '.') condition2
    val1ColumWithoutDot = Data.List.drop 1 val1Column
    val2ColumWithoutDot = Data.List.drop 1 val2Column
    col1Valid = not (Data.List.null val1ColumWithoutDot) && not (Data.List.null val1Table)
    col2Valid = not (Data.List.null val2ColumWithoutDot) && not (Data.List.null val2Table)


statementClause' :: [String] -> TableName -> Either ErrorMessage (Maybe WhereClause)
statementClause' afterWhere tableName = do
  case Data.List.length afterWhere of
    0 -> do
      Right Nothing
    _ -> do
      tryParseWhereClause' afterWhere tableName

tryParseWhereClause' :: [String] -> TableName -> Either ErrorMessage (Maybe WhereClause)
tryParseWhereClause' afterWhere tableName
  | isBoolIsTrueFalseClauseLike = case splitStatementToWhereIsClause' afterWhere tableName of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | isAndClauseLike = case parseWhereAnd' (Data.List.drop 1 afterWhere) tableName of
    Left err -> Left err
    Right clause -> Right $ Just clause
  | otherwise = Left "Failed to parse where clause. Where clause type not implemented or recognised. Please only use `where and` and `where bool is true/false`"
  where
    afterWhereWithoutWhere = Data.List.drop 1 afterWhere
    (_, afterIs) = Data.List.break (== "is") afterWhereWithoutWhere
    firstElementIsColumn = not (Data.List.null afterWhereWithoutWhere) && (Data.List.length (wordsWhen (== '.') (Data.List.head afterWhereWithoutWhere)) == 1)
    isBoolIsTrueFalseClauseLike = Data.List.length afterWhereWithoutWhere == 3 && Data.List.length afterIs == 2 && (afterIs Data.List.!! 1 == "false" || afterIs Data.List.!! 1 == "true") && firstElementIsColumn
    isAndClauseLike = Data.List.null afterIs

splitStatementToWhereIsClause' :: [String] -> TableName -> Either ErrorMessage WhereClause
splitStatementToWhereIsClause' [boolColName, "is", boolString] tableName = Right $ IsValueBool parsedBoolString tableName $ Data.List.init boolColName
  where
    parsedBoolString = boolString == "true"
splitStatementToWhereIsClause' _ _ = Left "Unsupported or invalid where bool is true false clause"


parseWhereAnd' :: [String] -> TableName -> Either ErrorMessage WhereClause
parseWhereAnd' afterWhere tableName
  | matchesWhereAndPattern' afterWhere = splitStatementToAndClause afterWhere tableName
  | otherwise = Left "Unable to parse where and clause"
  where
    splitStatementToAndClause :: [String] -> TableName -> Either ErrorMessage WhereClause
    splitStatementToAndClause strList tableName' = do
      conditionList <- getConditionList strList tableName'
      getConditions conditionList

    getConditions :: [Condition] -> Either ErrorMessage WhereClause
    getConditions conditions = Right $ Conditions conditions

    getConditionList :: [String] -> TableName -> Either ErrorMessage [Condition]
    getConditionList [condition1, operator, condition2] tableName'' = do
      condition <- getCondition' condition1 operator condition2 tableName''
      return [condition]

    getConditionList (condition1 : operator : condition2 : "and" : xs) tableName'' = do
      conditionBase <- getCondition' condition1 operator condition2 tableName''
      rest <- if not (Data.List.null xs) then getConditionList xs tableName'' else Right []
      return $ conditionBase : rest

    getConditionList _ _ = Left "Error parsing where and clause"

getConditionValue' :: String -> Either ErrorMessage ConditionValue
getConditionValue' condition
  | isNumber condition = Right $ IntValue (read condition :: Integer)
  | Data.List.length condition > 2 && "'" `Data.List.isPrefixOf` condition && "'" `Data.List.isSuffixOf` condition = Right $ StrValue (Data.List.drop 1 (Data.List.init condition))
  | otherwise = Left "Error parsing condition value"

getCondition' :: String -> String -> String -> TableName -> Either ErrorMessage Condition
getCondition' val1 op val2 tableName
  | op == "=" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ Equals (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "=" && isJust value1Type && isNothing value2Type && isCondition1= Right $ Equals (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == ">" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThan (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == ">" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThan (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == "<" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessThan (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "<" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessThan (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == ">=" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ GreaterThanOrEqual (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == ">=" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ GreaterThanOrEqual (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == "<=" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ LessthanOrEqual (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "<=" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ LessthanOrEqual (TableColumn tableName val2) $ castEither defaultCondition condition1
  | op == "<>" && isJust value2Type && isNothing value1Type && isCondition2 = Right $ NotEqual (TableColumn tableName val1) $ castEither defaultCondition condition2
  | op == "<>" && isJust value1Type && isNothing value2Type && isCondition1 = Right $ NotEqual (TableColumn tableName val2) $ castEither defaultCondition condition1
  | otherwise = Left "Error parsing where and condition. Only able to compare integer and string values with columns"
  where
    value1Type = parseType val1
    value2Type = parseType val2
    condition1 = getConditionValue' val1
    condition2 = getConditionValue' val2
    defaultCondition = StrValue "Kas skaitys tas gaidys (Isskyrus destytoja)"

    isCondition1 = case condition1 of
      Right _ -> True
      Left _ -> False

    isCondition2 = case condition2 of
      Right _ -> True
      Left _ -> False


matchesWhereAndPattern' :: [String] -> Bool
matchesWhereAndPattern' [condition1, operator, condition2] = isWhereAndOperation' condition1 operator condition2
matchesWhereAndPattern' (condition1 : operator : condition2 : andString : xs) = matchesWhereAndPattern' [condition1, operator, condition2] && andString == "and" && matchesWhereAndPattern' xs
matchesWhereAndPattern' _ = False

isWhereAndOperation' :: String -> String -> String -> Bool
isWhereAndOperation' condition1 operator condition2
  | Data.List.elem operator [">", "<", "=", "<>", "<=", ">="] && (col1Valid || col2Valid) = True
  | otherwise = False
  where
    col1Valid = not (Data.List.null condition1)
    col2Valid = not (Data.List.null condition2)

parseType :: String -> Maybe ColumnType
parseType str
  | isNumber str = Just IntegerType
  | "'" `Data.List.isPrefixOf` str && "'" `Data.List.isSuffixOf` str = Just StringType
  | otherwise = Nothing

isNumber :: String -> Bool
isNumber "" = False
isNumber xs =
  case Data.List.dropWhile isDigit xs of
    "" -> True
    _ -> False

parseSemiCaseSensitive :: String -> [String]
parseSemiCaseSensitive statement = convertedStatement
  where
    splitStatement = Data.List.words statement
    convertedStatement = Data.List.map wordToLowerSensitive splitStatement

wordToLowerSensitive :: String -> String
wordToLowerSensitive word
  | Data.List.map Data.Char.toLower word `Data.List.elem` keywords = Data.List.map Data.Char.toLower word
  | "avg(" `Data.List.isPrefixOf` Data.List.map Data.Char.toLower word && ")" `Data.List.isSuffixOf` word = "avg(" Data.List.++ Data.List.drop 4 (Data.List.init word) Data.List.++ ")"
  | "max(" `Data.List.isPrefixOf` Data.List.map Data.Char.toLower word && ")" `Data.List.isSuffixOf` word = "max(" Data.List.++ Data.List.drop 4 (Data.List.init word) Data.List.++ ")"
  | otherwise = word
  where
    keywords = ["select", "from", "where", "show", "table", "tables", "false", "true", "and", "is", "insert", "delete", "update", "set", "into", "values"]


-- ===============================================================
-- End of Parser


-- ===============================================================
  --Start of folterRows


filterRows :: DataFrame -> Maybe WhereClause -> Either String DataFrame
filterRows df@(DataFrame cols _) (Just wc) =
    if whereClauseHasValidColumns wc cols
    then filterDataFrame df wc
    else Left "Error: Specified column in WhereClause does not exist."
filterRows (DataFrame cols _) Nothing = Right $ DataFrame cols []

whereClauseHasValidColumns :: WhereClause -> [Column] -> Bool
whereClauseHasValidColumns (IsValueBool _ _ columnName) cols = isJust (findColumnIndex columnName cols)
whereClauseHasValidColumns (Conditions conditions) cols = Data.List.all (`conditionHasValidColumn` cols) conditions

conditionHasValidColumn :: Condition -> [Column] -> Bool
conditionHasValidColumn condition cols = isJust (findColumnIndex (columnNameFromCondition condition) cols)

columnNameFromCondition :: Condition -> String
columnNameFromCondition (Equals colName _) = extractColumnName colName
columnNameFromCondition (GreaterThan colName _) = extractColumnName colName
columnNameFromCondition (LessThan colName _) = extractColumnName colName
columnNameFromCondition (LessthanOrEqual colName _) = extractColumnName colName
columnNameFromCondition (GreaterThanOrEqual colName _) = extractColumnName colName
columnNameFromCondition (NotEqual colName _) = extractColumnName colName

extractColumnName :: SelectColumn -> String
extractColumnName (TableColumn _ colName) = colName

extractColumnName _ = error "Unsupported SelectColumn type for condition"


filterDataFrame :: DataFrame -> WhereClause -> Either String DataFrame
filterDataFrame (DataFrame cols rows) wc =
    let filteredRows = Data.List.filter (rowSatisfiesWhereClause wc cols) rows
    in if Data.List.null filteredRows
       then Left "Error: No rows match the specified condition."
       else Right $ DataFrame cols filteredRows

rowSatisfiesWhereClause :: WhereClause -> [Column] -> Row -> Bool
rowSatisfiesWhereClause (IsValueBool b _ columnName) cols row =
    case findColumnIndex columnName cols of
        Just idx -> case row !! idx of
            BoolValue val -> val == b
            _ -> False
        Nothing -> False
rowSatisfiesWhereClause (Conditions conditions) cols row =
    Data.List.all (`conditionSatisfied` (cols, row)) conditions

conditionSatisfied :: Condition -> ([Column], Row) -> Bool
conditionSatisfied (Equals colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (==) condValue
conditionSatisfied (GreaterThan colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (>) condValue
conditionSatisfied (LessThan colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (<) condValue
conditionSatisfied (LessthanOrEqual colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (<=) condValue
conditionSatisfied (GreaterThanOrEqual colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (>=) condValue
conditionSatisfied (NotEqual colName condValue) (cols, row) =
    compareWithCondition (columnNameFromSelectColumn colName) cols row (/=) condValue

columnNameFromSelectColumn :: SelectColumn -> String
columnNameFromSelectColumn (TableColumn _ colName) = colName
columnNameFromSelectColumn _ = error "Unsupported SelectColumn type for condition"

compareWithCondition :: String -> [Column] -> Row -> (forall a. Ord a => a -> a -> Bool) -> ConditionValue -> Bool
compareWithCondition colName cols row op condValue =
    case findColumnIndex colName cols of
        Just idx -> compareValue (row !! idx) op condValue
        Nothing -> False

compareValue :: Value -> (forall a. Ord a => a -> a -> Bool) -> ConditionValue -> Bool
compareValue (IntegerValue i) op (IntValue v) = i `op` v
compareValue (StringValue s) op (StrValue v) = s `op` v
compareValue _ _ _ = False

findColumnIndex :: String -> [Column] -> Maybe Int
findColumnIndex columnName = Data.List.findIndex (\(Column name _) -> name == columnName)

-- Start of insert 
-- =============================================

createRowFromValues :: [ColumnName] -> [Column] -> [Value] -> Either ErrorMessage Row

createRowFromValues insertCols [col] values = do
  if not (null insertCols || null values) && getColumnName col `elem` insertCols
    then do
      let colIndex = fromMaybe (-1) $ Data.List.elemIndex (getColumnName col) insertCols
      element <- Right $ values !! colIndex
      return [element]
    else
      return [NullValue]

createRowFromValues insertCols (col : xs) values = do
  if not (null insertCols || null values) && getColumnName col `elem` insertCols
    then do
      let colIndex = fromMaybe (-1) (Data.List.elemIndex (getColumnName col) insertCols)
      element <- Right $ values !! colIndex
      rest <- createRowFromValues (removeAtIndex colIndex insertCols) xs (removeAtIndex colIndex values)
      return $ element : rest
    else do
      rest <- createRowFromValues insertCols xs values
      return $ NullValue : rest


createRowFromValues _ _ _ = Left "Should not be here"

getColumnName :: Column -> String
getColumnName (Column name _) = name

removeAtIndex :: Int -> [a] -> [a]
removeAtIndex index list
    | index < 0 || index >= length list = list
    | otherwise = take index list ++ drop (index + 1) list


-- End of Insert 
-- ========================================================================

extractColumnNames :: SelectedColumns -> [ColumnName]
extractColumnNames = mapMaybe extractName
  where
    extractName :: SelectColumn -> Maybe ColumnName
    extractName (TableColumn _ colName) = Just colName
    extractName _ = Nothing

instance Ord Value where
compare (IntegerValue x) (IntegerValue y) = Prelude.compare x y
compare (StringValue x) (StringValue y) = Prelude.compare x y
compare (BoolValue x) (BoolValue y) = Prelude.compare x y
compare _ _ = EQ

extractColumns :: DataFrame -> [Column]
extractColumns (DataFrame cols _) = cols

extractRows :: DataFrame -> [Row]
extractRows (DataFrame _ rows) = rows

extractSelectedColumnsRows :: [Lib3.TableName] -> [Lib3.SelectColumn] -> [(Lib3.TableName, DataFrame)] -> [Row]
extractSelectedColumnsRows selectedTables selectedColumns tables =
    concatMap extractRowsFromTable filteredTables
    where
        filteredTables = filter (\(name, _) -> name `elem` selectedTables) tables

        extractRowsFromTable :: (Lib3.TableName, DataFrame) -> [Row]
        extractRowsFromTable (_, df) = map (extractSelectedCols df) (extractRows df)

        extractSelectedCols :: DataFrame -> Row -> Row
        extractSelectedCols df row = mapMaybe (extractColumn df row) selectedColumns

        extractColumn :: DataFrame -> Row -> Lib3.SelectColumn -> Maybe Value
        extractColumn df row col = case col of
            Lib3.TableColumn _ columnName -> lookupValue columnName df row
            _ -> Nothing

        lookupValue :: String -> DataFrame -> Row -> Maybe Value
        lookupValue columnName df row = do
            colIndex <- findIndex (\(Column name _) -> name == columnName) (extractColumns df)
            Just (row !! colIndex)

extractAggregateRows :: [Lib3.TableName] -> [Lib3.SelectColumn] -> Maybe Lib3.WhereClause -> [(Lib3.TableName, DataFrame)] -> [Row]
extractAggregateRows tableNames aggFuncs whereClause tables =
    let filteredTables = filter (\(name, _) -> name `elem` tableNames) tables
        aggregatedRows = map (applyAggregateFunction aggFuncs filteredTables whereClause) aggFuncs
    in [concat aggregatedRows]

applyAggregateFunction :: [Lib3.SelectColumn] -> [(Lib3.TableName, DataFrame)] -> Maybe Lib3.WhereClause -> Lib3.SelectColumn -> [Value]
applyAggregateFunction _ tables whereClause aggFunc =
    case aggFunc of
        Lib3.Max tableName colName -> [maxAggregate tableName colName tables whereClause]
        Lib3.Avg tableName colName -> [avgAggregate tableName colName tables whereClause]
        _ -> error "Unsupported aggregate function"

maxAggregate :: Lib3.TableName -> String -> [(Lib3.TableName, DataFrame)] -> Maybe Lib3.WhereClause -> Value
maxAggregate tableName colName tables whereClause =
    let relevantRows = extractRelevantRows tableName colName tables whereClause
        maxVal = maximum $ mapMaybe unwrapInteger relevantRows
    in IntegerValue maxVal

avgAggregate :: Lib3.TableName -> String -> [(Lib3.TableName, DataFrame)] -> Maybe Lib3.WhereClause -> Value
avgAggregate tableName colName tables whereClause =
    let relevantRows = extractRelevantRows tableName colName tables whereClause
        values = mapMaybe unwrapInteger relevantRows
        avgVal = sum values `div` fromIntegral (length values)
    in IntegerValue avgVal

extractRelevantRows :: Lib3.TableName -> String -> [(Lib3.TableName, DataFrame)] -> Maybe Lib3.WhereClause -> [Value]
extractRelevantRows tableName colName tables whereClause =
    let maybeTable = lookup tableName tables
        maybeColumnIndex = maybeTable >>= \df -> findColumnIndex colName (extractColumns df)
    in case (maybeTable, maybeColumnIndex) of
        (Just df, Just colIndex) -> filterRowsByWhereClause df colIndex whereClause
        _ -> []

filterRowsByWhereClause :: DataFrame -> Int -> Maybe Lib3.WhereClause -> [Value]
filterRowsByWhereClause (DataFrame columns rows) colIndex whereClause =
    case whereClause of
        Just wc -> map (!! colIndex) $ filter (rowSatisfiesWhereClause1 wc columns) rows
        Nothing -> map (!! colIndex) rows

rowSatisfiesWhereClause1 :: Lib3.WhereClause -> [Column] -> Row -> Bool
rowSatisfiesWhereClause1 (Lib3.Conditions conditions) columns row =
  all (`conditionSatisfiedInRow` (columns, row)) conditions
rowSatisfiesWhereClause1 (Lib3.IsValueBool b _ columnName) columns row =
      case findColumnIndex columnName columns of
          Just idx -> case row !! idx of
              BoolValue val -> val == b
              _ -> False
          Nothing -> False

conditionSatisfiedInRow :: Lib3.Condition -> ([Column], Row) -> Bool
conditionSatisfiedInRow (Lib3.Equals colName condValue) = evaluateCondition (==) colName condValue
conditionSatisfiedInRow (Lib3.GreaterThan colName condValue) = evaluateCondition (>) colName condValue
conditionSatisfiedInRow (Lib3.LessThan colName condValue) = evaluateCondition (<) colName condValue
conditionSatisfiedInRow (Lib3.LessthanOrEqual colName condValue) = evaluateCondition (<=) colName condValue
conditionSatisfiedInRow (Lib3.GreaterThanOrEqual colName condValue) = evaluateCondition (>=) colName condValue
conditionSatisfiedInRow (Lib3.NotEqual colName condValue) = evaluateCondition (/=) colName condValue

evaluateCondition :: (Value -> Value -> Bool) -> Lib3.SelectColumn -> Lib3.ConditionValue -> ([Column], Row) -> Bool
evaluateCondition op (Lib3.TableColumn _ colName) condValue (columns, row) =
    case findColumnIndex colName columns of
        Just idx -> compareConditionValue (row !! idx) op (convertConditionValue condValue)
        Nothing -> False
evaluateCondition _ _ _ _ = False

compareConditionValue :: Value -> (Value -> Value -> Bool) -> Value -> Bool
compareConditionValue val1 op val2 = val1 `op` val2

convertConditionValue :: Lib3.ConditionValue -> Value
convertConditionValue (Lib3.IntValue i) = IntegerValue i
convertConditionValue (Lib3.StrValue s) = StringValue s

unwrapInteger :: Value -> Maybe Integer
unwrapInteger (IntegerValue i) = Just i
unwrapInteger _ = Nothing